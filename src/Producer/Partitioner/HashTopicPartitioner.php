<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Partitioner;

use Thesis\Kafka\Producer\Hasher;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Producer\StickyTopicPartitioner;
use Thesis\Kafka\Producer\TopicPartitioner;

/**
 * @api
 */
final readonly class HashTopicPartitioner implements StickyTopicPartitioner
{
    public function __construct(
        private Hasher $hasher,
        private TopicPartitioner $whenKeyIsNull,
    ) {}

    #[\Override]
    public function partition(Record $record, int $partitions): int
    {
        if ($record->key === null) {
            return $this->whenKeyIsNull->partition($record, $partitions);
        }

        // Mask the sign bit before modulo which matches Java's
        // `Utils.toPositive(hash) % numPartitions` behavior. Required
        // because PHP's `%` keeps the dividend's sign, which would
        // produce negative partition indices for negative hashes.
        return ($this->hasher->hash($record->key) & 0x7FFFFFFF) % $partitions;
    }

    #[\Override]
    public function rotate(int $partition, int $partitions): void
    {
        // HashTopicPartitioner itself isn't sticky — the key pins the
        // partition. But our null-key fallback might be (RoundRobin /
        // Random), so we forward the event if it cares.
        if ($this->whenKeyIsNull instanceof StickyTopicPartitioner) {
            $this->whenKeyIsNull->rotate($partition, $partitions);
        }
    }
}
