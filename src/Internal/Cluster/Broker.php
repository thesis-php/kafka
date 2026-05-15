<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

use Amp\Cancellation;
use Thesis\Kafka\Internal;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 */
final readonly class Broker
{
    public const int NOT_A_BROKER_ID = -1;

    public static function unknown(
        Internal\Connection $connection,
    ): self {
        return new self(
            id: self::NOT_A_BROKER_ID,
            connection: $connection,
        );
    }

    public function __construct(
        public int $id,
        public Internal\Connection $connection,
    ) {}

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws KafkaException
     */
    public function request(Generated\Request $request, Cancellation $cancellation): Generated\Response
    {
        return$this->connection->call(
            new Internal\Connection\Call($request),
            $cancellation,
        );
    }
}
