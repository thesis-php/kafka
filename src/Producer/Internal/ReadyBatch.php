<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\DeferredFuture;
use Amp\Future;
use Thesis\Kafka\Producer;

/**
 * @internal
 */
final readonly class ReadyBatch
{
    /** @var DeferredFuture<null> */
    private DeferredFuture $done;

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
    ) {
        $this->done = new DeferredFuture();
    }

    /**
     * Completes when every record of the batch has a result. It never fails.
     *
     * @return Future<null>
     */
    public function done(): Future
    {
        return $this->done->getFuture();
    }

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

        $this->done->complete();
    }

    public function fail(\Throwable $error): void
    {
        foreach ($this->futures as $future) {
            $future->error($error);
        }

        $this->done->complete();
    }
}
