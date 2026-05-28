<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\DeferredFuture;
use Thesis\Kafka\Producer;

/**
 * @internal
 */
final readonly class ReadyBatch
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param list<DeferredFuture<Producer\Produced>> $futures
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public Protocol\RecordBatch $batch,
        public array $futures,
    ) {}

    /**
     * @param ?non-negative-int $offset
     * @param ?non-negative-int $timestamp
     */
    public function complete(?int $offset, ?int $timestamp): void
    {
        foreach ($this->futures as $idx => $future) {
            $future->complete(new Producer\Produced(
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
