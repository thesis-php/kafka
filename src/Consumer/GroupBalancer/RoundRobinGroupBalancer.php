<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\GroupBalancer;

use Thesis\Kafka\Consumer\ClassicGroupBalancer;
use Thesis\Kafka\Consumer\GroupMember;
use Thesis\Kafka\Consumer\RebalanceProtocol;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @api
 */
final readonly class RoundRobinGroupBalancer implements ClassicGroupBalancer
{
    #[\Override]
    public function name(): string
    {
        return 'roundrobin';
    }

    #[\Override]
    public function protocol(): RebalanceProtocol
    {
        return RebalanceProtocol::Eager;
    }

    #[\Override]
    public function memberMetadata(
        array $topics,
        array $ownedPartitions,
        int $generationId,
    ): Generated\ConsumerProtocolSubscription {
        return new Generated\ConsumerProtocolSubscription(
            topics: $topics,
            userData: null,
            ownedPartitions: [],
            generationId: $generationId,
            rackId: null,
        );
    }

    #[\Override]
    public function assign(array $members, array $partitionCounts): array
    {
        // Seed every member with an empty assignment so members that get
        // nothing still have a key for SyncGroup.
        $result = [];
        foreach ($members as $member) {
            $result[$member->memberId] = [];
        }

        // Flatten every (topic, partition) of every subscribed topic into one
        // ordered list. Unlike Range (which slices each topic separately), RoundRobin
        // lays all partitions across all topics in a single sequence and
        // deals them out in turn. Sort topics for determinism: only the leader
        // runs this, but the result must not depend on map ordering.
        $topics = array_keys($partitionCounts);
        sort($topics);

        /** @var list<TopicPartition> $allPartitions */
        $allPartitions = [];
        foreach ($topics as $topic) {
            $partitionCount = $partitionCounts[$topic] ?? 0;
            for ($partition = 0; $partition < $partitionCount; ++$partition) {
                $allPartitions[] = new TopicPartition($topic, $partition);
            }
        }

        // Sorted member ids — the round-robin ring. Deterministic for the same
        // reason as above.
        $memberIds = array_map(
            static fn(GroupMember $member) => $member->memberId,
            $members,
        );
        sort($memberIds);

        // Which topics each member subscribed to: a partition only goes to a
        // member that actually wants its topic.
        $subscriptions = [];
        foreach ($members as $member) {
            $subscriptions[$member->memberId] = $member->topics;
        }

        // Walk partitions in order: for each advance around the member ring
        // until we find one subscribed to that partition's topic, and give it
        // there. The cursor persists across partitions, so assignment rotates
        // evenly rather than restarting per partition.
        $cursor = 0;
        $memberCount = \count($memberIds);

        foreach ($allPartitions as $tp) {
            // Find the next member (from the current cursor) subscribed to this
            // topic. Bounded by memberCount: if no subscriber exists, skip the
            // partition rather than loop forever.
            for ($i = 0; $i < $memberCount; ++$i) {
                $memberId = $memberIds[$cursor % $memberCount]; // @phpstan-ignore offsetAccess.notFound
                $cursor = ($cursor + 1) % $memberCount;

                if (\in_array($tp->topic, $subscriptions[$memberId] ?? [], true)) {
                    $result[$memberId][] = $tp;
                    break;
                }
            }
        }

        return $result;
    }
}
