<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\TopicPartition;

/**
 * Tracks the fetch position and leader of every assigned partition.
 * Shared between the fetch loops (read where to fetch, advance after
 * fetch) and the recovery of {@see FetchCycle}. A partition without a
 * leader waits for the recovery.
 *
 * @internal
 */
final class SubscriptionState
{
    /** @var TopicPartition\Map<non-negative-int> */
    private TopicPartition\Map $positions;

    /** @var TopicPartition\Map<non-negative-int> */
    private TopicPartition\Map $leaders;

    /**
     * Waiting partitions whose position is out of range.
     *
     * @var TopicPartition\Map<true>
     */
    private TopicPartition\Map $resets;

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
        $this->resets = new TopicPartition\Map();
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
     * The offset the next fetch for this partition will request.
     *
     * @return non-negative-int
     */
    public function position(TopicPartition $tp): int
    {
        return $this->positions[$tp];
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
     * The partition is not fetched until {@see follow()}.
     *
     * @param bool $reset the position is out of range and needs a reset
     */
    public function release(TopicPartition $tp, bool $reset = false): void
    {
        unset($this->leaders[$tp]);

        if ($reset) {
            $this->resets[$tp] = true;
        }
    }

    /**
     * The partition is fetched from this leader again.
     *
     * @param non-negative-int $leader
     */
    public function follow(TopicPartition $tp, int $leader): void
    {
        $this->leaders[$tp] = $leader;
        unset($this->resets[$tp]);
    }

    /**
     * @return list<TopicPartition> the partitions without a leader
     */
    public function waiting(): array
    {
        $waiting = [];
        foreach ($this->positions as $tp) {
            if (!isset($this->leaders[$tp])) {
                $waiting[] = $tp;
            }
        }

        return $waiting;
    }

    public function resetting(TopicPartition $tp): bool
    {
        return isset($this->resets[$tp]);
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
