<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Cluster;

use Amp\Cancellation;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception\NoCompatibleRequestVersion;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Runtime;

/**
 * @internal
 */
final readonly class Broker
{
    public const int NOT_A_BROKER_ID = -1;

    public static function unknown(
        Runtime\Connection $connection,
        Endpoint $endpoint,
        NegotiatedVersions $versions = new NegotiatedVersions(),
    ): self {
        return new self(
            id: self::NOT_A_BROKER_ID,
            connection: $connection,
            endpoint: $endpoint,
            versions: $versions,
        );
    }

    public function __construct(
        public int $id,
        public Runtime\Connection $connection,
        public Endpoint $endpoint,
        public NegotiatedVersions $versions,
    ) {}

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws KafkaException
     */
    public function request(Generated\Request $request, Cancellation $cancellation): Generated\Response
    {
        $future = $this->connection->call(new Runtime\Call(
            request: $request,
            version: $this->versions->versionOf($request->apiKey())
                ?? throw new NoCompatibleRequestVersion($request->apiKey()),
        ));

        return $future->await($cancellation);
    }
}
