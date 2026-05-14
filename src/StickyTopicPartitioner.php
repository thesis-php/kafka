<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * Optional extension for TopicPartitioner that want to "stick" to one
 * partition until its batch is full, then move to a different one.
 *
 * The Producer calls rotate() when the batch on $partition has just
 * been produced, signalling that the partitioner should
 * pick something else next time. Implementations should:
 *   - use $partition to know which partition to move away from
 *     (typically: pick anything except $partition);
 *   - use $partitions because the partition count may have changed
 *     since the last partition() call (topic was resized).
 *
 * Only meaningful for selection strategies with no per-record opinion
 * — round-robin and random. Hash-based partitioners ignore this hook
 * (the key already pins the partition).
 *
 * @api
 */
interface StickyTopicPartitioner extends TopicPartitioner
{
    /**
     * @param non-negative-int $partition partition the just-closed batch belonged to
     * @param positive-int $partitions current partition count
     */
    public function rotate(int $partition, int $partitions): void;
}
