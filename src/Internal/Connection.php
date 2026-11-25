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
use Amp\TimeoutCancellation;
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
     * back-pressure: concurrent callers wait on push if the writer
     * is slower than the producers. Each caller waits with its own cancellation.
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

    /**
     * The hrtime() of the last bytes read from the socket.
     */
    private int $lastReadAt;

    /**
     * Requests whose callers stopped waiting before the answer came.
     * The broker still owes an answer to each of them.
     *
     * @var array<non-negative-int, true>
     */
    private array $abandoned = [];

    /**
     * The hrtime() when the connection began to wait for an answer: the
     * start of the first request after a moment with no owed answers.
     */
    private int $waitingSince = 0;

    /**
     * Fails with the close reason when the connection closes. A caller that
     * waits for a free place in the send queue also waits for it, so the
     * close releases the caller. The queue itself does not release it.
     *
     * @var DeferredFuture<never>
     */
    private readonly DeferredFuture $closed;

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
        $this->lastReadAt = (int) hrtime(true);
        $this->closed = new DeferredFuture();
        // Most connections close with nobody waiting for a place in the queue.
        $this->closed->getFuture()->ignore();
    }

    /**
     * @template R of Generated\Response
     * @param Connection\Call<R> $call
     * @param ?int $startedAt the hrtime() when the caller began the request, now by default
     * @return ($reply is Connection\Reply::Expected ? R : null)
     * @throws Exception
     * @throws CancelledException
     */
    public function call(
        Connection\Call $call,
        Cancellation $cancellation = new NullCancellation(),
        Connection\Reply $reply = Connection\Reply::Expected,
        ?int $startedAt = null,
    ): mixed {
        $startedAt ??= (int) hrtime(true);

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

        // The connection can close while the session is refreshed.
        // A completed send queue does not accept a request.
        if ($this->state === Connection\State::Closed) {
            throw new ConnectionClosed("Connection is closed: {$this->endpoint}.");
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

        /** @var DeferredFuture<null|R> $deferred */
        $deferred = new DeferredFuture();
        // A caller can stop waiting before the close fails this request.
        $deferred->getFuture()->ignore();

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
            if ($this->pending === [] && $this->abandoned === []) {
                $this->waitingSince = $startedAt;
            }

            $this->pending[$id] = $request;
        }

        try {
            // A full queue makes the caller wait. It waits with its own cancellation,
            // and the close of the connection ends the wait with the close reason.
            Future\awaitFirst([
                $this->send->pushAsync($request)->ignore(),
                $this->closed->getFuture(),
            ], $cancellation);

            return $deferred->getFuture()->await($cancellation);
        } catch (\Throwable $e) {
            if ($e instanceof CancelledException) {
                $this->closeIfSilent((int) hrtime(true));
            }

            if ($e instanceof ConnectionClosed) {
                // @infection-ignore-all
                $this->logger->debug('request aborted by connection close', [
                    'correlationId' => $id,
                    'apiKey' => $call->request->apiKey()->name,
                    'apiVersion' => $requestVersion,
                    'reason' => $e->getMessage(),
                ]);
            } elseif (!$e instanceof CancelledException) {
                // @infection-ignore-all
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
            // pipelined request can only be done by killing the whole TCP session.
            // We do it only for a connection that does not answer (see silentSince()).
            if ($reply === Connection\Reply::Expected) {
                unset($this->pending[$id]);

                // The answer has not come, and the connection is open. The answer is
                // still owed, so the connection keeps waiting for it.
                if (!$deferred->isComplete()) {
                    $this->abandoned[$id] = true;
                }
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
                // @infection-ignore-all
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

    /**
     * Closes the connection when it owes answers and has been silent for the
     * request timeout: nothing was read since it began to wait, or since the
     * last bytes. Such a connection is probably half-open. A connection with
     * no owed answers is never silent, however long it was idle.
     *
     * The timeout of a caller starts a little before its request reaches the
     * connection. So the silence starts at the moment the caller began the
     * request ($startedAt of call()), not when the request was queued. The
     * next request of the same caller then sees the silence of its timed-out
     * request as at least the request timeout.
     *
     * @param int $now the hrtime() of the check
     * @return bool true when the connection was closed
     */
    public function closeIfSilent(int $now): bool
    {
        if ($this->state === Connection\State::Closed || ($this->pending === [] && $this->abandoned === [])) {
            return false;
        }

        $silence = $now - max($this->waitingSince, $this->lastReadAt);
        if ($silence < $this->config->requestTimeout->toNanoseconds()) {
            return false;
        }

        // @infection-ignore-all
        $this->logger->warning('connection does not answer, closing it', [
            'silence' => round($silence / 1e9, 3),
            'timeout' => $this->config->requestTimeout->toSeconds(3),
        ]);

        $this->close();

        return true;
    }

    public function close(): void
    {
        $this->shutdown(new ConnectionClosed("Connection closed locally: {$this->endpoint}."));
    }

    /**
     * Closes the connection once. Every waiting request and every onClose
     * callback gets the reason at once, so nobody uses a dead connection.
     */
    private function shutdown(Exception $reason): void
    {
        if ($this->state === Connection\State::Closed) {
            return;
        }

        $this->state = Connection\State::Closed;
        $this->socket->close();
        $this->send->complete();
        $this->closed->error($reason);

        $requests = $this->pending;
        $this->pending = [];
        $this->abandoned = [];

        foreach ($requests as $request) {
            $request->error($reason);
        }

        $callbacks = $this->onClose;
        $this->onClose = [];

        foreach ($callbacks as $callback) {
            $callback($reason);
        }
    }

    /**
     * Returns a library exception for a failure of the socket.
     */
    private function failure(\Throwable $e): Exception
    {
        return $e instanceof Exception
            ? $e
            : new ConnectionClosed("Connection failed: {$this->endpoint}: {$e->getMessage()}");
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

        // A cancelled caller only stops waiting. The exchange goes on for the other callers.
        ($this->refreshSession ??= $this->startRefreshSession())->await($cancellation);
    }

    /**
     * @return Future<null>
     */
    private function startRefreshSession(): Future
    {
        $future = async(function (): void {
            try {
                // The exchange is shared by all waiting callers, so it has its own time limit.
                $this->authenticate(new TimeoutCancellation($this->config->requestTimeout->toSeconds(3)));
            } catch (\Throwable $e) {
                // Only a failed exchange closes the connection.
                $this->close();

                if ($e instanceof CancelledException) {
                    throw new ConnectionClosed("Re-authentication timed out: {$this->endpoint}.");
                }

                throw $e;
            } finally {
                $this->refreshSession = null;
            }
        });

        // All callers can stop waiting before the exchange ends.
        // Its error must not reach the event loop then.
        $future->ignore();

        return $future;
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
     *
     * Both close the connection when they stop. They never throw into the event loop.
     */
    private function run(): void
    {
        EventLoop::queue($this->read(...));
        EventLoop::queue($this->write(...));

        $this->state = Connection\State::Running;
    }

    private function read(): void
    {
        $buffer = new Connection\ReadBuffer();
        $reason = null;

        try {
            while (($chunk = $this->socket->read()) !== null) {
                $this->lastReadAt = (int) hrtime(true);

                foreach ($buffer->push($chunk) as $frame) {
                    $this->dispatch($frame);
                }
            }
        } catch (\Throwable $e) {
            // Without the cause the waiting requests would get only "connection closed".
            $reason = $this->failure($e);
        } finally {
            // The socket is gone, so no response will arrive.
            // The broker closed it, or close() did, or the read failed.
            $this->shutdown($reason ?? new ConnectionClosed("Connection closed by the broker: {$this->endpoint}."));
        }
    }

    private function write(): void
    {
        $buffer = new Connection\WriteBuffer($this->socket);

        try {
            /** @var Connection\PendingRequest<*> $request */
            foreach ($this->send->iterate() as $request) {
                $buffer->write($request);
            }
        } catch (\Throwable $e) {
            if ($this->state === Connection\State::Running) {
                // @infection-ignore-all
                $this->logger->debug('write failed, closing the connection', [
                    'exception' => $e,
                ]);
            }

            $this->shutdown($this->failure($e));
        }
    }

    /**
     * @param non-empty-string $frame
     * @throws Exception
     */
    private function dispatch(string $frame): void
    {
        $reader = new Reader($frame);

        $correlationId = $reader->readInt32();
        unset($this->abandoned[$correlationId]);

        if (!isset($this->pending[$correlationId])) {
            // Either the request was cancelled before the response arrived
            // (entry removed from $pending), or the broker is misbehaving.
            // Either way, drop silently and move on.
            return;
        }

        /** @var Connection\PendingRequest<*> $request */
        $request = $this->pending[$correlationId];
        unset($this->pending[$correlationId]);

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
