<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\NullCancellation;
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
final readonly class OffsetCommitter
{
    public function __construct(
        private Cluster $cluster,
        private Coordinator $coordinator,
        private Consumer\GroupConfig $group,
    ) {}

    /**
     * @param TopicPartition\Map<non-negative-int> $offsets
     * @throws Exception
     */
    public function commit(
        TopicPartition\Map $offsets,
        Membership $membership,
        Cancellation $cancellation = new NullCancellation(),
    ): void {
        if (\count($offsets) === 0) {
            return;
        }

        $metadata = $this->cluster->metadata($cancellation);

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
            $cancellation,
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new Exception\ProtocolError($errors);
        }
    }
}
