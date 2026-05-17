<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Producer;

use Amp\DeferredFuture;
use Thesis\Kafka\ProduceResult;
use Thesis\Kafka\Protocol\Record\RecordBatch;

/**
 * @internal
 */
final readonly class ReadyBatch
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param list<DeferredFuture<ProduceResult>> $futures
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public RecordBatch $batch,
        public array $futures,
    ) {}

    /**
     * @param ?non-negative-int $offset
     * @param ?non-negative-int $timestamp
     */
    public function complete(?int $offset, ?int $timestamp): void
    {
        foreach ($this->futures as $idx => $future) {
            $future->complete(new ProduceResult(
                topic: $this->topic,
                partition: $this->partition,
                offset: $offset !== null ? $offset + $idx : null,
                timestamp: $timestamp > 0 ? $timestamp : null,
            ));
        }
    }

    public function fail(\Throwable $error): void
    {
        foreach ($this->futures as $future) {
            $future->error($error);
        }
    }
}
