<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Ramsey\Uuid\Uuid;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final class OffsetCommitter
{
    /**
     * Delivered-but-not-yet-committed position per partition: the next offset to
     * read (last delivered offset + 1). {@see mark()} advances it as batches are
     * handed to the caller. A successful commit prunes what it persisted, so it
     * holds exactly what {@see commitUncommitted()} still needs to send.
     *
     * @var TopicPartition\Map<non-negative-int>
     */
    private TopicPartition\Map $committable;

    public function __construct(
        private readonly Cluster $cluster,
        private readonly Coordinator $coordinator,
        private readonly Consumer\GroupConfig $group,
    ) {
        $this->committable = new TopicPartition\Map();
    }

    /**
     * Remember a delivered batch as pending commit progress so a later
     * {@see commitUncommitted()} knows what to persist. For each {@see TopicPartition}
     * the batch touches, keep the highest delivered offset + 1 (the next offset to read).
     *
     * @param non-empty-list<Consumer\Record> $batch
     */
    public function mark(array $batch): void
    {
        foreach (self::offsets($batch)->iter() as $tp => $offset) {
            $this->committable[$tp] = max($this->committable[$tp] ?? 0, $offset);
        }
    }

    /**
     * Commit exactly the given records: for each {@see TopicPartition} the highest
     * record offset + 1, i.e. the next offset to read.
     *
     * @param non-empty-list<Consumer\Record> $records
     * @throws Exception
     */
    public function commitRecords(array $records, Membership $membership): void
    {
        $this->doCommit(self::offsets($records), $membership);
    }

    /**
     * Commit every tracked partition the membership still owns — all progress
     * delivered since the last commit. A no-op when nothing new was delivered.
     *
     * @throws Exception
     */
    public function commitUncommitted(Membership $membership): void
    {
        if (\count($this->committable) === 0) {
            return;
        }

        /** @var TopicPartition\Map<true> $owned */
        $owned = new TopicPartition\Map();
        foreach ($membership->assignments as $tp) {
            $owned[$tp] = true;
        }

        /** @var TopicPartition\Map<non-negative-int> $offsets */
        $offsets = new TopicPartition\Map();
        foreach ($this->committable as $tp) {
            if (isset($owned[$tp])) {
                $offsets[$tp] = $this->committable[$tp];
            }
        }

        $this->doCommit($offsets, $membership);
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $offsets
     * @throws Exception
     */
    private function doCommit(TopicPartition\Map $offsets, Membership $membership): void
    {
        if (\count($offsets) === 0) {
            return;
        }

        $metadata = $this->cluster->metadata();

        /** @var array<non-empty-string, list<Generated\OffsetCommitRequest\OffsetCommitRequestTopic\OffsetCommitRequestPartition>> $byTopic */
        $byTopic = [];
        foreach ($offsets as $tp) {
            $byTopic[$tp->topic][] = new Generated\OffsetCommitRequest\OffsetCommitRequestTopic\OffsetCommitRequestPartition(
                partitionIndex: $tp->partition,
                committedOffset: $offsets[$tp],
                committedLeaderEpoch: -1,
                committedMetadata: null,
            );
        }

        $topics = [];
        foreach ($byTopic as $topic => $partitions) {
            $topics[] = new Generated\OffsetCommitRequest\OffsetCommitRequestTopic(
                name: $topic,
                topicId: $metadata->topics[$topic]->topicId ?? Uuid::fromString(Uuid::NIL),
                partitions: $partitions,
            );
        }

        $response = $this->coordinator->request(
            new Generated\OffsetCommitRequest(
                groupId: $this->group->groupId,
                generationIdOrMemberEpoch: $membership->generationId,
                memberId: $membership->memberId,
                groupInstanceId: $this->group->groupInstanceId,
                retentionTimeMs: -1, // broker default
                topics: $topics,
            ),
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new Exception\ProtocolError($errors);
        }

        // Persisted: drop the positions we just committed so a later commit won't
        // resend them. A concurrent delivery may have advanced a partition past
        // what we committed — keep those, they're still pending.
        foreach ($offsets as $tp) {
            if (isset($this->committable[$tp]) && $this->committable[$tp] <= $offsets[$tp]) {
                unset($this->committable[$tp]);
            }
        }
    }

    /**
     * @param non-empty-list<Consumer\Record> $records
     * @return TopicPartition\Map<non-negative-int>
     */
    private static function offsets(array $records): TopicPartition\Map
    {
        /** @var TopicPartition\Map<non-negative-int> $offsets */
        $offsets = new TopicPartition\Map();

        foreach ($records as $record) {
            $tp = $record->tp();
            $offsets[$tp] = max($offsets[$tp] ?? 0, $record->offset + 1);
        }

        return $offsets;
    }
}
