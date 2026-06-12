<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * A balancer for the classic {@see Generated\JoinGroupRequest, Generated\SyncGroupRequest} protocol, where members
 * exchange metadata and the elected leader computes the assignment
 * client-side. Covers both eager and cooperative rebalancing — the
 * difference is in {@see protocol()} and whether owned partitions are
 * carried in the metadata.
 *
 * Not used with KIP-848, where the broker assigns server-side and only the
 * protocol name is needed.
 *
 * @api
 */
interface ClassicGroupBalancer extends GroupBalancer
{
    /**
     * Eager (revoke everything, then reassign) or cooperative (revoke only
     * what moves). Drives how the consumer releases partitions on rebalance.
     */
    public function protocol(): RebalanceProtocol;

    /**
     * Build this member's {@see Generated\JoinGroupRequest} metadata: the subscription bytes the
     * coordinator distributes to the leader. Carries the member's topics
     * and, for cooperative balancers, the partitions it currently owns.
     *
     * @param non-empty-list<non-empty-string> $topics this member's subscription
     * @param array<non-empty-string, list<int>> $ownedPartitions currently owned (cooperative), empty for eager
     */
    public function memberMetadata(
        array $topics,
        array $ownedPartitions,
        int $generationId,
    ): Generated\ConsumerProtocolSubscription;

    /**
     * Compute the assignment, run by the group leader during SyncGroup.
     *
     * @param non-empty-list<GroupMember> $members
     * @param array<non-empty-string, int> $partitionCounts partitions per topic
     * @return array<non-empty-string, list<TopicPartition>> partitions per member id
     */
    public function assign(array $members, array $partitionCounts): array;
}
