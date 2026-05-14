<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Thesis\Kafka\ProducerRecord;
use Thesis\Kafka\StickyTopicPartitioner;

/**
 * @api
 */
final class RoundRobinTopicPartitioner implements StickyTopicPartitioner
{
    /**
     * Currently "stuck" partition. -1 means no partition is chosen yet:
     * the next partition() call will start the cycle at 0. Advanced by
     * rotate() after each batch is produced so successive batches walk
     * around the ring.
     */
    private int $current = -1;

    #[\Override]
    public function partition(ProducerRecord $record, int $partitions): int
    {
        // Reuse the stuck partition while it's still valid. The partition
        // count can shrink between calls if the topic is reconfigured, in
        // which case our previously chosen index might be out of range.
        if ($this->current < 0 || $this->current >= $partitions) {
            $this->current = 0;
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

        // Step forward by one, wrapping back to 0 after the last partition.
        $this->current = ($this->current + 1) % $partitions;
    }
}
