<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

use Thesis\Kafka\RecordHeader;
use Thesis\Kafka\TopicPartition;

/**
 * @api
 */
final readonly class Record
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param non-negative-int $offset
     * @param list<RecordHeader> $headers
     * @param int $timestamp in milliseconds
     * @param int $leaderEpoch partition leader epoch of the batch, -1 if unknown
     * @param int $producerId id of the producer that wrote the batch, -1 if none
     * @param int $producerEpoch epoch of that producer, -1 if none
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public int $offset,
        public ?string $key,
        public ?string $value,
        public array $headers,
        public int $timestamp,
        public int $leaderEpoch,
        public int $producerId,
        public int $producerEpoch,
    ) {}

    public function tp(): TopicPartition
    {
        return new TopicPartition($this->topic, $this->partition);
    }
}
