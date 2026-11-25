<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\TimeoutCancellation;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Exception\ConnectionFailed;
use Thesis\Kafka\Internal;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final class Broker
{
    /** @var \SplObjectStorage<Connection\Kind, Connection> */
    private \SplObjectStorage $alive;

    /** @var \SplObjectStorage<Connection\Kind, Future<Connection>> */
    private \SplObjectStorage $opening;

    /**
     * Stops the connects in progress when the broker is closed.
     */
    private DeferredCancellation $closing;

    /**
     * @param TimeSpan $connectTimeout the time limit of a new connection with its handshake
     */
    public function __construct(
        public readonly NodeId $nodeId,
        public readonly Endpoint $endpoint,
        private readonly Internal\ConnectionFactory $factory,
        private readonly TimeSpan $connectTimeout,
    ) {
        $this->alive = new \SplObjectStorage();
        $this->opening = new \SplObjectStorage();
        $this->closing = new DeferredCancellation();
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return ($reply is Connection\Reply::Expected ? T : null)
     * @throws Exception
     * @throws CancelledException
     */
    public function request(
        Generated\Request $request,
        Cancellation $cancellation,
        Connection\Reply $reply = Connection\Reply::Expected,
    ): mixed {
        // The silence of a connection is measured from this moment. See Connection::closeIfSilent().
        $startedAt = (int) hrtime(true);

        $kind = Connection\Kind::for($request);

        $connection = $this->alive[$kind] ?? null;

        // A connection that does not answer is closed. Its onClose callback drops it from alive[].
        if ($connection?->closeIfSilent($startedAt) === true) {
            $connection = null;
        }

        // Fast path: reuse a healthy connection of this kind.
        // Slow paths in order:
        //   - someone is already opening one — await their Future;
        //   - no one is — start opening, publish the Future for any
        //     concurrent caller, and await it ourselves.
        // The connect is shared, so it does not use the cancellation of one
        // caller. Each caller stops waiting on its own cancellation.
        $connection ??= ($this->opening[$kind] ??= $this->connect($kind))->await($cancellation);

        return $connection->call(new Connection\Call($request), $cancellation, $reply, $startedAt);
    }

    public function alive(): bool
    {
        return \count($this->alive) > 0;
    }

    public function close(Cancellation $cancellation = new NullCancellation()): void
    {
        $this->closing->cancel();
        $this->closing = new DeferredCancellation();

        $futures = [];

        while ($this->alive->valid()) {
            $kind = $this->alive->current();
            $connection = $this->alive[$kind];
            unset($this->alive[$kind]);

            $futures[] = async($connection->close(...));
        }

        Future\awaitAll($futures, $cancellation);
    }

    /**
     * @return Future<Connection>
     */
    private function connect(Connection\Kind $kind): Future
    {
        $alive = $this->alive;
        $opening = $this->opening;
        $factory = $this->factory;
        $endpoint = $this->endpoint;
        $closing = $this->closing->getCancellation();
        $timeout = $this->connectTimeout->toSeconds(3);

        /** @var Future<Connection> $future */
        $future = async(static function () use (
            $alive,
            $opening,
            $factory,
            $kind,
            $endpoint,
            $closing,
            $timeout,
        ): Connection {
            try {
                try {
                    $connection = $factory->open(
                        $endpoint,
                        new CompositeCancellation($closing, new TimeoutCancellation($timeout)),
                    );
                } catch (CancelledException $e) {
                    // The callers did not cancel this connect, so they get a network error.
                    if ($closing->isRequested()) {
                        throw new ConnectionClosed("Broker is closed: {$endpoint}.");
                    }

                    throw new ConnectionFailed($endpoint, $e->getPrevious() ?? $e);
                }

                if ($closing->isRequested()) {
                    $connection->close();

                    throw new ConnectionClosed("Broker is closed: {$endpoint}.");
                }

                $alive[$kind] = $connection;

                // When this connection dies, drop it from
                // alive[] so the next request() lazily opens a fresh one.
                $connection->onClose(static function () use (
                    $alive,
                    $kind,
                ): void {
                    unset($alive[$kind]);
                });

                return $connection;
            } finally {
                // Clear the in-progress slot whether we succeeded or threw.
                // On failure, callers awaiting the Future will see the
                // exception via the Future itself; clearing $opening just
                // lets the next request() try a fresh open.
                unset($opening[$kind]);
            }
        });

        // All callers can stop waiting before the connect ends.
        // Its error must not reach the event loop then.
        $future->ignore();

        return $future;
    }
}
