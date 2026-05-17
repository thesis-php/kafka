<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Config\ProducerConfig;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final class Client
{
    private readonly Internal\Cluster $cluster;

    private ?Producer $producer = null;

    public function __construct(
        public readonly Config $config,
    ) {
        $this->cluster = new Internal\Cluster($config);
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

    /**
     * @return Future<ProduceResult>
     * @throws KafkaException
     */
    public function produce(ProduceRecord $record, Cancellation $cancellation = new NullCancellation()): Future
    {
        $this->producer ??= new Producer(
            $this->cluster,
            $this->config->producer ?? new ProducerConfig(),
        );

        return $this->producer->produce($record, $cancellation);
    }

    public function close(): void
    {
        $this->producer?->close();
        $this->cluster->close();
    }
}
