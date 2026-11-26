<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;

/**
 * A nullable struct has a marker byte before it: -1 for null, 1 for a value.
 */
final readonly class NullableStructTest
{
    #[Test]
    public function brokerReadsNullStructInRequest(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);

            $response = $kafka->client->request(new Generated\DescribeTopicPartitionsRequest(
                topics: [new Generated\DescribeTopicPartitionsRequest\TopicRequest($topic)],
                responsePartitionLimit: 2000,
                cursor: null,
            ));

            \assert(\count($response->topics) === 1);
            Assert::same($response->topics[0]->errorCode, 0);
            Assert::same($response->topics[0]->name, $topic);
            Assert::same(
                array_map(
                    static fn(Generated\DescribeTopicPartitionsResponse\DescribeTopicPartitionsResponseTopic\DescribeTopicPartitionsResponsePartition $partition) => [$partition->partitionIndex, $partition->errorCode],
                    $response->topics[0]->partitions,
                ),
                [[0, 0], [1, 0], [2, 0]],
            );
            Assert::null($response->nextCursor);
        });
    }

    #[Test]
    public function clientReadsNullStructInResponse(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2);
            $groupId = $kafka->groupId();
            $memberId = Uuid::uuid4()->toString();

            $metadata = $kafka->client->request(Kafka::metadataRequest([$topic]));
            \assert(\count($metadata->topics) === 1);
            $topicId = $metadata->topics[0]->topicId;

            // The first heartbeat joins the group with no partitions. A null field of a later heartbeat means "no change".
            $heartbeat = static fn(int $memberEpoch, ?Generated\ConsumerGroupHeartbeatRequest\TopicPartitions $owned = null): Generated\ConsumerGroupHeartbeatResponse => $kafka->client->request(
                new Generated\ConsumerGroupHeartbeatRequest(
                    groupId: $groupId,
                    memberId: $memberId,
                    memberEpoch: $memberEpoch,
                    instanceId: null,
                    rackId: null,
                    rebalanceTimeoutMs: $memberEpoch === 0 ? 30_000 : -1,
                    subscribedTopicNames: $memberEpoch === 0 ? [$topic] : null,
                    subscribedTopicRegex: null,
                    serverAssignor: null,
                    topicPartitions: match (true) {
                        $memberEpoch === 0 => [],
                        $owned === null => null,
                        default => [$owned],
                    },
                ),
            );

            // A new broker loads the offsets topic first. Until then the coordinator is not ready.
            $joined = $kafka->poll(static fn() => $heartbeat(0))
                ->expecting('a joined member')
                ->describedBy(static fn(Generated\ConsumerGroupHeartbeatResponse $response) => "error code {$response->errorCode}")
                ->until(static fn(Generated\ConsumerGroupHeartbeatResponse $response) => $response->errorCode === 0);

            Assert::same($joined->memberId, $memberId);
            Assert::int($joined->memberEpoch)->greaterThan(0);

            // The coordinator can assign the partitions with the join or with a later heartbeat.
            // The first assignment can be empty when the coordinator does not see the new topic yet.
            $hasPartitions = static fn(Generated\ConsumerGroupHeartbeatResponse $response): bool => $response->errorCode === 0
                && ($response->assignment->topicPartitions ?? []) !== [];

            $epoch = $joined->memberEpoch;
            $assigned = $hasPartitions($joined) ? $joined : $kafka
                ->poll(static function () use ($heartbeat, &$epoch): Generated\ConsumerGroupHeartbeatResponse {
                    $response = $heartbeat($epoch);
                    $epoch = $response->memberEpoch;

                    return $response;
                })
                ->expecting('a heartbeat response with an assignment')
                ->describedBy(static fn(Generated\ConsumerGroupHeartbeatResponse $response) => "error code {$response->errorCode}")
                ->until($hasPartitions);

            Assert::same($assigned->memberId, $memberId);
            Assert::instanceOf($assigned->assignment, Generated\ConsumerGroupHeartbeatResponse\Assignment::class);
            Assert::same(
                array_map(
                    static function (Generated\ConsumerGroupHeartbeatResponse\TopicPartitions $partitions): array {
                        $indexes = $partitions->partitions;
                        sort($indexes);

                        return [$partitions->topicId->toString(), $indexes];
                    },
                    $assigned->assignment->topicPartitions,
                ),
                [[$topicId->toString(), [0, 1]]],
            );

            $next = $heartbeat(
                $assigned->memberEpoch,
                new Generated\ConsumerGroupHeartbeatRequest\TopicPartitions($topicId, [0, 1]),
            );

            Assert::null($next->assignment);
            Assert::same($next->errorCode, 0);
            Assert::same($next->memberId, $memberId);
            Assert::same($next->memberEpoch, $assigned->memberEpoch);
        });
    }
}
