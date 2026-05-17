<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Internal;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;
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

    public function __construct(
        public readonly NodeId $nodeId,
        public readonly Endpoint $endpoint,
        private readonly Internal\ConnectionFactory $factory,
    ) {
        $this->alive = new \SplObjectStorage();
        $this->opening = new \SplObjectStorage();
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return ($reply is Connection\Reply::Expected ? T : null)
     * @throws KafkaException
     */
    public function request(
        Generated\Request $request,
        Cancellation $cancellation,
        Connection\Reply $reply = Connection\Reply::Expected,
    ): mixed {
        $kind = Connection\Kind::for($request);

        // Fast path: reuse a healthy connection of this kind.
        // Slow paths in order:
        //   - someone is already opening one — await their Future;
        //   - no one is — start opening, publish the Future for any
        //     concurrent caller, and await it ourselves.
        $connection = $this->alive[$kind]
            ?? ($this->opening[$kind] ??= $this->connect($kind, $cancellation))->await($cancellation);

        return $connection->call(new Connection\Call($request), $cancellation, $reply);
    }

    public function alive(): bool
    {
        return \count($this->alive) > 0;
    }

    public function close(Cancellation $cancellation = new NullCancellation()): void
    {
        $futures = [];

        while ($this->alive->valid()) {
            $kind = $this->alive->current();
            $connection = $this->alive[$kind];
            unset($this->alive[$kind]);

            $futures[] = async($connection->close(...));
        }

        Future\await($futures, $cancellation);
    }

    /**
     * @return Future<Connection>
     */
    private function connect(Connection\Kind $kind, Cancellation $cancellation): Future
    {
        $alive = $this->alive;
        $opening = $this->opening;
        $factory = $this->factory;
        $endpoint = $this->endpoint;

        /** @var Future<Connection> */
        return async(static function () use (
            $alive,
            $opening,
            $factory,
            $kind,
            $endpoint,
            $cancellation,
        ): Connection {
            try {
                $connection = $factory->open(
                    $endpoint,
                    $cancellation,
                );

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
    }
}
