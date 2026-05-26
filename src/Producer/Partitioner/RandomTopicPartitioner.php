<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Partitioner;

use Random\Randomizer;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Producer\StickyTopicPartitioner;

/**
 * @api
 */
final class RandomTopicPartitioner implements StickyTopicPartitioner
{
    /**
     * Currently "stuck" partition. -1 means no partition is chosen yet:
     * the next partition() call will pick a fresh one. Reset by rotate()
     * after each batch is produced so successive batches don't all
     * pile up on the same partition.
     */
    private int $current = -1;

    public function __construct(
        private readonly Randomizer $randomizer,
    ) {}

    #[\Override]
    public function partition(Record $record, int $partitions): int
    {
        // Reuse the stuck partition while it's still valid. The partition
        // count can shrink between calls if the topic is reconfigured, in
        // which case our previously chosen index might be out of range.
        if ($this->current < 0 || $this->current >= $partitions) {
            $this->current = $this->randomizer->getInt(0, $partitions - 1);
        }

        /** @var non-negative-int */
        return $this->current;
    }

    #[\Override]
    public function rotate(int $partition, int $partitions): void
    {
        // If the closed batch wasn't the one we were stuck to,
        // someone else is driving and we have nothing to rotate. Should
        // not happen in normal flow.
        if ($partition !== $this->current) {
            return;
        }

        if ($partitions === 1) {
            return;
        }

        // Pick a non-zero offset and walk that many steps around the ring of
        // partitions. Excluding offset 0 is what guarantees we don't land on
        // $current again: (current + k) mod n equals current only when k is a
        // multiple of n, and k here is in [1, n-1].
        $offset = $this->randomizer->getInt(1, $partitions - 1);
        $this->current = ($this->current + $offset) % $partitions;
    }
}
