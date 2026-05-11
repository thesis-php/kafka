<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\Socket\Socket;
use Revolt\EventLoop;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Exception\MalformedResponse;
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
     * @return R
     * @throws KafkaException
     * @throws Pipeline\DisposedException
     */
    public function call(
        Connection\Call $call,
        Cancellation $cancellation = new NullCancellation(),
    ): Generated\Response {
        if ($this->state === Connection\State::Closed) {
            throw new ConnectionClosed('Connection is closed.');
        }

        if ($this->state === Connection\State::Idle) {
            $this->run();
        }

        // A Call may carry an explicit version (used during the bootstrap
        // ApiVersions handshake, where we hard-code v0 because the negotiated
        // map isn't filled yet). For every normal request, version is unset
        // and we resolve it from the connection's version map.
        $requestVersion = $call->requestVersion;
        if ($call->requestVersionUnset()) {
            $requestVersion = $this->versions->versionOf($call->request->apiKey())
                ?? throw new NoCompatibleRequestVersion($call->request->apiKey());
        }

        \assert($requestVersion >= 0);

        $responseVersion = $requestVersion;
        if (!$call->responseVersionUnset()) {
            $responseVersion = $call->responseVersion;
        }

        /** @var DeferredFuture<R> $deferred */
        $deferred = new DeferredFuture();

        $id = $this->correlation->id;

        $request = new Connection\PendingRequest(
            request: $call->request,
            deferred: $deferred,
            correlationId: $id,
            requestVersion: $requestVersion,
            responseVersion: $responseVersion,
        );

        $pending = &$this->pending;
        $pending[$id] = $request;

        $this->send->push($request);

        try {
            return $deferred->getFuture()->await($cancellation);
        } finally {
            // Cancellation only removes the request from the pending map. If the
            // request was already written to the socket (very likely once we're
            // past push()), the response will still arrive and silently be dropped
            // in the read loop as "unknown correlation id". Aborting an in-flight
            // pipelined request can only be done by killing the whole TCP session,
            // which we don't do at this layer.
            unset($pending[$id]);
        }
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

                        $versions = [$request->requestVersion];

                        // ApiVersions bootstrap is the only place where we may decode the
                        // same response frame against two candidate body versions.
                        //
                        // We first try the version that was actually sent on the wire and,
                        // if that decode does not fit the received bytes, retry exactly
                        // once against the same frame using the alternate bootstrap
                        // response version. Re-parsing the already buffered frame is
                        // cheaper than issuing a second network request just to discover
                        // which body shape the broker chose for this response.
                        //
                        // We keep this special case local to ApiVersions. Once the
                        // ApiVersions handshake succeeds, the connection has negotiated
                        // versions for all subsequent APIs and normal decoding becomes a
                        // single-pass operation with one known body version.
                        if ($request->request->apiKey() === Generated\ApiKey::ApiVersions && $request->requestVersion !== $request->responseVersion) {
                            $versions[] = $request->responseVersion;
                        }

                        $last = array_key_last($versions);

                        foreach ($versions as $index => $version) {
                            try {
                                $headerVersion = $request
                                    ->request
                                    ->apiKey()
                                    ->responseHeaderVersion($version);

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

                                $responseType = $request
                                    ->request
                                    ->responseType();

                                $response = $responseType::decode($reader, $version);
                                $request->complete($response); // @phpstan-ignore argument.type

                                break;
                            } catch (\Throwable $exception) {
                                if (!$exception instanceof MalformedResponse || $index === $last) {
                                    $request->error($exception);

                                    break;
                                }

                                // Older brokers can answer a newer ApiVersions request with a
                                // legacy-shaped response body. Likewise, decoding a legacy
                                // body as a modern flexible version can fail either by
                                // exhausting the frame or by tripping a compact/non-compact
                                // invariant. Rather than send a second request, rewind to
                                // the start of the response body (right after correlationId)
                                // and reinterpret this same frame using the alternate
                                // version.
                                $reader->reset(4);
                            }
                        }
                    }
                }
            } catch (\Throwable $exception) {
                // Caught here so the finally block can rethrow it through pending
                // requests with proper context. Without this, an unexpected error
                // in the read loop would surface in pending requests as a generic
                // "connection closed" with no clue about the actual cause.
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
