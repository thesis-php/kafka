<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\Socket\Socket;
use Revolt\EventLoop;
use Thesis\Kafka\Config;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Exception\MalformedResponse;
use Thesis\Kafka\Exception\NoCompatibleRequestVersion;
use Thesis\Kafka\Exception\NoCompatibleSaslMechanism;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\SASL;
use function Amp\async;

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

    /** @var ?Future<null> */
    private ?Future $refreshSession = null;

    private ?Time\Duration $sessionExpires = null;

    public function __construct(
        private readonly Socket $socket,
        private readonly Config $config,
        public readonly Endpoint $endpoint,
        private readonly ContextLogger $logger,
        private readonly NegotiatedVersions $versions = new NegotiatedVersions(),
        int $sendBufferSize = self::SEND_BUFFER_SIZE,
    ) {
        $this->correlation = new Connection\CorrelationIds();
        $this->send = new Pipeline\Queue(bufferSize: $sendBufferSize);
    }

    /**
     * @template R of Generated\Response
     * @param Connection\Call<R> $call
     * @return ($reply is Connection\Reply::Expected ? R : null)
     * @throws Exception
     * @throws Pipeline\DisposedException
     * @throws CancelledException
     */
    public function call(
        Connection\Call $call,
        Cancellation $cancellation = new NullCancellation(),
        Connection\Reply $reply = Connection\Reply::Expected,
    ): mixed {
        if ($this->state === Connection\State::Closed) {
            throw new ConnectionClosed('Connection is closed.');
        }

        if ($this->state === Connection\State::Idle) {
            $this->run();
        }

        // Re-authentication is triggered "synchronously" at the point where a
        // caller is about to send a new application request. This matches
        // KIP-368 more naturally than trying to refresh from the background
        // reader / writer loops: once the SASL session has expired, the next
        // ordinary request must wait until the connection is re-authenticated.
        //
        // refreshSession() is safe to call concurrently. Internally it
        // coalesces concurrent refresh attempts behind one shared Future, so
        // only one coroutine performs the SASL exchange while all other
        // callers await the same result.
        $this->refreshSession($cancellation, $call->request->apiKey());

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

        /** @var DeferredFuture<null|R> $deferred */
        $deferred = new DeferredFuture();

        if ($reply === Connection\Reply::None) {
            // Resolve immediately. The write is queued; we don't await
            // its completion at the application level. Used mostly for Acks::None.
            $deferred->complete(null);
        }

        $id = $this->correlation->id;

        $request = new Connection\PendingRequest(
            request: $call->request,
            deferred: $deferred,
            correlationId: $id,
            requestVersion: $requestVersion,
            responseVersion: $responseVersion,
        );

        if ($reply === Connection\Reply::Expected) {
            $this->pending[$id] = $request;
        }

        $this->send->push($request);

        try {
            return $deferred->getFuture()->await($cancellation);
        } catch (\Throwable $e) {
            if ($e instanceof ConnectionClosed) {
                $this->logger->debug('request aborted by connection close', [
                    'correlationId' => $id,
                    'apiKey' => $call->request->apiKey()->name,
                    'apiVersion' => $requestVersion,
                    'reason' => $e->getMessage(),
                ]);
            } elseif (!$e instanceof CancelledException) {
                $this->logger->error('receive response error', [
                    'correlationId' => $id,
                    'apiKey' => $call->request->apiKey()->name,
                    'apiVersion' => $requestVersion,
                    'exception' => $e,
                ]);
            }

            throw $e;
        } finally {
            // Cancellation only removes the request from the pending map. If the
            // request was already written to the socket (very likely once we're
            // past push()), the response will still arrive and silently be dropped
            // in the read loop as "unknown correlation id". Aborting an in-flight
            // pipelined request can only be done by killing the whole TCP session,
            // which we don't do at this layer.
            if ($reply === Connection\Reply::Expected) {
                unset($this->pending[$id]);
            }
        }
    }

    /**
     * @throws Exception
     */
    public function versionsHandshake(Cancellation $cancellation): void
    {
        // ApiVersions is the only request we can send before the connection has
        // any negotiated protocol state. We start at v4 because modern brokers
        // support it and can return richer version metadata. If the broker
        // answers with UNSUPPORTED_VERSION, we fall back once to v0 — the
        // conservative baseline expected to work on older brokers.
        //
        // The Connection read path already knows how to reinterpret the same
        // ApiVersions response frame across legacy and modern body shapes, so
        // this method only decides whether a second wire request is necessary.
        //
        // We do not share negotiated versions across connections: each new
        // socket performs its own bootstrap and seeds its own version map
        // (KIP-511).
        $request = new Generated\ApiVersionsRequest(
            clientSoftwareName: $this->config->clientSoftwareName,
            clientSoftwareVersion: $this->config->clientSoftwareVersion,
        );

        $versions = [4, 0];
        $last = array_key_last($versions);

        foreach ($versions as $index => $version) {
            $response = $this
                ->call(
                    new Connection\Call(
                        request: $request,
                        requestVersion: $version,
                        responseVersion: 0,
                    ),
                    $cancellation,
                );

            $error = ErrorCode::from($response->errorCode);
            if ($error === ErrorCode::None) {
                $this->versions->seed(...$response->apiKeys);

                return;
            }

            if ($error !== ErrorCode::UnsupportedVersion || $index === $last) {
                throw new ProtocolError(Errors::for($response));
            }
        }
    }

    /**
     * @throws Exception
     */
    public function authenticate(Cancellation $cancellation): void
    {
        $sasl = $this->config->sasl->authenticators ?? [];
        if ($sasl === []) {
            return;
        }

        $supported = [];

        // Kafka requires the client to nominate a SASL mechanism in the
        // SaslHandshake request itself. We therefore try the configured
        // authenticators in order and, if the broker replies with
        // UNSUPPORTED_SASL_MECHANISM plus a mechanism list, use that list to
        // prune the remaining candidates.
        foreach ($sasl as $authenticator) {
            if ($supported !== [] && !\in_array($authenticator->mechanism(), $supported, true)) {
                continue;
            }

            $response = $this
                ->call(
                    new Connection\Call(
                        request: new Generated\SaslHandshakeRequest(
                            mechanism: $authenticator->mechanism(),
                        ),
                    ),
                    $cancellation,
                );

            $error = ErrorCode::from($response->errorCode);
            if ($error === ErrorCode::None) {
                $this->doSaslAuthenticate($authenticator, $cancellation);
                $this->logger->info('authenticated', [
                    'mechanism' => $authenticator->mechanism(),
                ]);

                return;
            }

            if ($error !== ErrorCode::UnsupportedSaslMechanism) {
                throw new ProtocolError(Errors::for($response));
            }

            $supported = $response->mechanisms;
        }

        throw new NoCompatibleSaslMechanism(
            array_map(
                static fn(SASL\Authenticator $authenticator) => $authenticator->mechanism(),
                $sasl,
            ),
            $supported,
        );
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

    private function refreshSession(Cancellation $cancellation, Generated\ApiKey $requestKey): void
    {
        // Session refresh itself is implemented with SaslHandshake /
        // SaslAuthenticate requests. Those internal protocol steps must bypass
        // the "refresh before ordinary request" guard, otherwise an expired
        // session would recursively try to refresh again while already inside
        // the refresh flow and deadlock on its own in-flight Future.
        if (\in_array($requestKey, [Generated\ApiKey::SaslHandshake, Generated\ApiKey::SaslAuthenticate], true)) {
            return;
        }

        if ($this->sessionExpires === null || $this->sessionExpires->inFuture()) {
            return;
        }

        try {
            $this->refreshSession ??= async($this->authenticate(...), $cancellation);
            $this->refreshSession->await($cancellation);
        } catch (\Throwable $e) {
            $this->close();

            throw $e;
        } finally {
            $this->refreshSession = null;
        }
    }

    /**
     * @throws Exception
     */
    private function doSaslAuthenticate(
        SASL\Authenticator $authenticator,
        Cancellation $cancellation,
    ): void {
        $session = $authenticator->authenticate();
        $exchange = $session->start();
        $step = 0;
        $requestDuration = Time\now();
        $sessionLifetimeMs = 0;

        // Once the mechanism has been accepted by SaslHandshake, authentication
        // proceeds as a challenge/response loop over the SaslAuthenticate API.
        // PLAIN finishes in one round-trip; SCRAM and future mechanisms can ask
        // for more steps by returning State::Continue from the local session.
        while ($exchange->message !== null) {
            $response = $this
                ->call(
                    new Connection\Call(
                        request: new Generated\SaslAuthenticateRequest(
                            authBytes: $exchange->message,
                        ),
                    ),
                    $cancellation,
                );

            if (ErrorCode::from($response->errorCode) !== ErrorCode::None) {
                throw new ProtocolError(Errors::for($response));
            }

            $sessionLifetimeMs = $response->sessionLifetimeMs;

            if ($exchange->state === SASL\State::Done) {
                break;
            }

            $exchange = $session->challenge(++$step, $response->authBytes);
        }

        if ($sessionLifetimeMs > 0) {
            // KIP-368 allows the broker to advertise a finite SASL session
            // lifetime. Once that deadline is reached, the connection must be
            // used for re-authentication before we send the next ordinary Kafka
            // request, so we turn the broker-provided lifetime into a monotonic
            // local deadline and intentionally schedule it a little early.
            $this->sessionExpires = Time\calculateSessionExpiration(
                requestStartMs: $requestDuration->milliseconds,
                requestDurationMs: $requestDuration->elapsed(),
                sessionLifetimeMs: $sessionLifetimeMs,
            );
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
