<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final readonly class Client
{
    private Internal\Cluster $cluster;

    private Internal\Producer $producer;

    public function __construct(
        public Config $config,
    ) {
        $this->cluster = new Internal\Cluster($config);
        $this->producer = new Internal\Producer(
            $this->cluster,
            $this->config->producer,
        );
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
     * @param ProduceRecord|non-empty-list<ProduceRecord> $records
     * @return ($records is non-empty-list<ProduceRecord> ? non-empty-list<Future<ProduceResult>> : Future<ProduceResult>)
     * @throws KafkaException
     */
    public function produce(ProduceRecord|array $records, Cancellation $cancellation = new NullCancellation()): mixed
    {
        if (\is_array($records)) {
            return array_map(
                fn(ProduceRecord $record) => $this->producer->produce($record, $cancellation),
                $records,
            );
        }

        return $this->producer->produce($records, $cancellation);
    }

    /**
     * @param ProduceRecord|non-empty-list<ProduceRecord> $records
     * @return ($records is non-empty-list<ProduceRecord> ? non-empty-list<ProduceResult> : ProduceResult)
     * @throws KafkaException
     */
    public function produceSync(ProduceRecord|array $records, Cancellation $cancellation = new NullCancellation()): mixed
    {
        $futures = $this->produce($records, $cancellation);
        if (\is_array($futures)) {
            /** @var non-empty-list<ProduceResult> */
            return Future\await($futures);
        }

        return $futures->await();
    }

    public function flush(): void
    {
        $this->producer->flush();
    }

    public function close(): void
    {
        $this->producer->close();
        $this->cluster->close();
    }
}
