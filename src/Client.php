<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\NullCancellation;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final readonly class Client
{
    private Runtime\Cluster $cluster;

    public function __construct(
        public Config $config,
    ) {
        $this->cluster = new Runtime\Cluster($config);
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws KafkaException
     */
    public function request(Generated\Request $request, ?Cancellation $cancellation = null): Generated\Response
    {
        return $this->cluster->request($request, $cancellation);
    }

    public function close(Cancellation $cancellation = new NullCancellation()): void
    {
        $this->cluster->close($cancellation);
    }
}
