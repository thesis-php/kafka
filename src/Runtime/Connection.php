<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\Socket\Socket;
use Revolt\EventLoop;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Exception\NoCompatibleRequestVersion;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Protocol\Reader;

/**
 * @internal
 */
final class Connection
{
    private const int SEND_BUFFER_SIZE = 10;

    private readonly Connection\CorrelationIds $correlation;

    /**
     * Outbound queue: one writer per push, single consumer is the writer
     * coroutine in run(). Bounded buffer ($sendBufferSize) provides
     * back-pressure: concurrent callers will block on push if the writer
     * is slower than the producers.
     *
     * @var Pipeline\Queue<Connection\PendingRequest<*>>
     */
    private readonly Pipeline\Queue $send;

    /**
     * In-flight requests keyed by correlationId. Populated when a call is
     * issued, drained in two ways:
     *   - the read loop completes the deferred when the response arrives,
     *   - cancellation removes the entry (the response, if it still arrives,
     *     will be dropped silently as "unknown correlation id").
     *
     * @var array<non-negative-int, Connection\PendingRequest<*>>
     */
    private array $pending = [];

    /** @var list<\Closure(\Throwable): void> */
    private array $onClose = [];

    private Connection\State $state = Connection\State::Idle;

    public function __construct(
        private readonly Socket $socket,
        public readonly Endpoint $endpoint,
        public readonly NegotiatedVersions $versions = new NegotiatedVersions(),
        int $sendBufferSize = self::SEND_BUFFER_SIZE,
    ) {
        $this->correlation = new Connection\CorrelationIds();
        $this->send = new Pipeline\Queue(bufferSize: $sendBufferSize);
    }

    /**
     * @template R of Generated\Response
     * @param Connection\Call<R> $call
     * @return Future<R>
     * @throws KafkaException
     * @throws Pipeline\DisposedException
     */
    public function call(
        Connection\Call $call,
        Cancellation $cancellation = new NullCancellation(),
    ): Future {
        if ($this->state === Connection\State::Closed) {
            throw new ConnectionClosed('Connection is closed.');
        }

        // A Call may carry an explicit version (used during the bootstrap
        // ApiVersions handshake, where we hard-code v0 because the negotiated
        // map isn't filled yet). For every normal request, version is unset
        // and we resolve it from the connection's version map.
        $version = $call->version;
        if ($call->versionUnset()) {
            $version = $this->versions->versionOf($call->request->apiKey())
                ?? throw new NoCompatibleRequestVersion($call->request->apiKey());
        }

        \assert($version >= 0);

        /** @var DeferredFuture<R> $deferred */
        $deferred = new DeferredFuture();

        $id = $this->correlation->id;

        $request = new Connection\PendingRequest(
            $call,
            $deferred,
            $id,
            $version,
        );

        $pending = &$this->pending;
        $pending[$id] = $request;

        // Cancellation only removes the request from the pending map. If the
        // request was already written to the socket (very likely once we're
        // past push()), the response will still arrive and silently be dropped
        // in the read loop as "unknown correlation id". Aborting an in-flight
        // pipelined request can only be done by killing the whole TCP session,
        // which we don't do at this layer.
        $cancellation->subscribe(static function (CancelledException $e) use (
            &$pending,
            $id,
            $deferred,
        ): void {
            unset($pending[$id]);
            $deferred->error($e);
        });

        if ($this->state === Connection\State::Idle) {
            $this->run();
        }

        $this->send->push($request);

        return $deferred->getFuture();
    }

    /**
     * @param \Closure(\Throwable): void $f
     * @throws ConnectionClosed
     */
    public function onClose(\Closure $f): void
    {
        if ($this->state === Connection\State::Closed) {
            throw new ConnectionClosed('Connection is closed.');
        }

        $this->onClose[] = $f;
    }

    public function close(): void
    {
        if ($this->state === Connection\State::Running) {
            $this->state = Connection\State::Closed;
            $this->socket->close();
            $this->send->complete();
        }
    }

    /**
     * Starts two coroutines on the event loop:
     *   - reader: reads framed responses from the socket, decodes the
     *     header, finds the matching pending request by correlationId,
     *     completes its deferred. Exits when the socket is closed.
     *   - writer: drains the outbound queue and writes framed requests
     *     into the socket. Exits when the queue is completed (close()).
     */
    private function run(): void
    {
        $pending = &$this->pending;
        $onClose = &$this->onClose;
        $state = &$this->state;
        $send = $this->send;
        $socket = $this->socket;
        $endpoint = $this->endpoint;

        EventLoop::queue(static function () use (
            &$pending,
            &$onClose,
            &$state,
            $socket,
            $endpoint,
            $send,
        ): void {
            $buffer = new Connection\ReadBuffer();

            try {
                while (($chunk = $socket->read()) !== null) {
                    foreach ($buffer->push($chunk) as $frame) {
                        $reader = new Reader($frame);

                        $correlationId = $reader->readInt32();
                        if (!isset($pending[$correlationId])) {
                            // Either the request was cancelled before the response arrived
                            // (entry removed from $pending), or the broker is misbehaving.
                            // Either way, drop silently and move on.
                            continue;
                        }

                        /** @var Connection\PendingRequest<*> $request */
                        $request = $pending[$correlationId];
                        unset($pending[$correlationId]);

                        try {
                            $headerVersion = $request->call->request
                                ->apiKey()
                                ->responseHeaderVersion($request->version);

                            // Response header v1+ ends with a flexible-versions tagged-fields
                            // trailer. We don't currently emit or interpret any tagged fields
                            // at the header level, so we just consume them.
                            //
                            // Note: ApiVersionsResponse is a special case in the protocol — its
                            // body uses flexible versions but its header is always v0. The
                            // responseHeaderVersion() method on ApiKey hardcodes that.
                            if ($headerVersion >= 1) {
                                $reader->readTaggedFields();
                            }

                            $request->complete($reader);
                        } catch (\Throwable $e) {
                            $request->error($e);
                        }
                    }
                }
            } catch (\Throwable $exception) {
            } finally {
                // The read loop has exited — the socket is gone, either because we
                // closed it ourselves (close() was called) or because the broker
                // closed it (idle timeout, network failure, broker shutdown).
                // Either way, every in-flight request is now lost: the response
                // will never arrive. Reject them so callers don't block forever.
                $requests = $pending;
                $pending = [];

                $callbacks = $onClose;
                $onClose = [];

                if ($requests !== [] || $callbacks !== []) {
                    $exception ??= $state === Connection\State::Running
                        ? new ConnectionClosed("Connection closed by the broker: {$endpoint}.")
                        : new ConnectionClosed("Connection closed locally: {$endpoint}.");

                    foreach ($requests as $request) {
                        $request->error($exception);
                    }

                    foreach ($callbacks as $callback) {
                        $callback($exception);
                    }
                }

                // Wake the writer coroutine if it's still draining the queue. When state
                // is Running, only the read loop has exited (broker closed the socket);
                // close() hasn't been called and hasn't completed the queue yet, so we
                // do it here. When state is Closed, close() already called complete()
                // and a second call would throw.
                if ($state === Connection\State::Running) {
                    $send->complete();
                }

                $state = Connection\State::Closed;
            }
        });

        EventLoop::queue(static function () use (
            $send,
            $socket,
        ): void {
            $buffer = new Connection\WriteBuffer($socket);

            $iterator = $send->iterate();

            /** @var Connection\PendingRequest<*> $request */
            foreach ($iterator as $request) {
                $buffer->write($request);
            }
        });

        $this->state = Connection\State::Running;
    }
}
