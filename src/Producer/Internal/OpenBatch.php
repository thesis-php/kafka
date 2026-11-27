<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\DeferredFuture;
use Thesis\Kafka\Producer;

/**
 * @internal
 */
final class OpenBatch implements \Countable
{
    /**
     * @param non-empty-string $id
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param positive-int $partitions the partition count of the topic when the batch was opened
     * @param list<DeferredFuture<Producer\Produced>> $futures one per appended record, indexed positionally
     */
    public function __construct(
        public readonly string $id,
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $partitions,
        public readonly Protocol\RecordBatch $batch,
        public private(set) OpenBatch\State $state = OpenBatch\State::Accumulate,
        public array $futures = [],
    ) {}

    public function release(): ReadyBatch
    {
        $this->state = OpenBatch\State::Released;

        return new ReadyBatch(
            topic: $this->topic,
            partition: $this->partition,
            batch: $this->batch,
            futures: $this->futures,
        );
    }

    #[\Override]
    public function count(): int
    {
        return $this->batch->count();
    }
}
