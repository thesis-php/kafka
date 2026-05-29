<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Ramsey\Uuid\UuidInterface;

/**
 * Tracks the fetch position and leader of every assigned partition.
 * Shared between the fetch loops (read where to fetch, advance after
 * fetch) and the consumer's seek surface.
 *
 * @internal
 */
final class SubscriptionState
{
    /** @var TopicPartition\Map<non-negative-int> */
    private TopicPartition\Map $positions;

    /** @var TopicPartition\Map<non-negative-int> */
    private TopicPartition\Map $leaders;

    /** @var array<non-empty-string, non-empty-string> */
    public private(set) array $topicsById = [];

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        public readonly array $topics,
    ) {
        $this->positions = new TopicPartition\Map();
        $this->leaders = new TopicPartition\Map();
    }

    /**
     * @param non-empty-string $topic
     */
    public function mapTopicId(UuidInterface $topicId, string $topic): void
    {
        $this->topicsById[$topicId->toString()] = $topic;
    }

    /**
     * @param non-negative-int $offset
     * @param non-negative-int $leader
     */
    public function assign(
        TopicPartition $tp,
        int $offset,
        int $leader,
    ): void {
        $this->positions[$tp] = $offset;
        $this->leaders[$tp] = $leader;
    }

    /**
     * @param non-negative-int $offset
     */
    public function advance(TopicPartition $tp, int $offset): void
    {
        $this->positions[$tp] = $offset;
    }

    /**
     * @param non-negative-int $offset
     */
    public function seek(TopicPartition $tp, int $offset): void
    {
        $this->positions[$tp] = $offset;
    }

    /**
     * Partitions led by the given broker, with their current positions.
     *
     * @return TopicPartition\Map<non-negative-int>
     */
    public function partitionsForLeader(int $leaderId): TopicPartition\Map
    {
        /** @var TopicPartition\Map<non-negative-int> $result */
        $result = new TopicPartition\Map();

        foreach ($this->leaders as $tp) {
            if ($this->leaders[$tp] === $leaderId) {
                $result[$tp] = $this->positions[$tp];
            }
        }

        return $result;
    }

    /**
     * @return list<non-negative-int>
     */
    public function leaders(): array
    {
        $ids = [];
        foreach ($this->leaders as $tp) {
            $ids[$this->leaders[$tp]] = true;
        }

        return array_keys($ids);
    }
}
