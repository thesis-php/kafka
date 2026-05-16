<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

/**
 * @internal
 */
final readonly class PartitionMetadata
{
    /**
     * @param int $leader -1 if no leader (mid-election)
     * @param list<int> $replicas all assigned replicas, leader included
     * @param list<int> $isr in-sync replicas
     */
    public function __construct(
        public int $partition,
        public int $leader,
        public array $replicas,
        public array $isr,
    ) {}

    /**
     * @param non-negative-int $brokerId
     */
    public function roleOf(int $brokerId): PartitionRole
    {
        if ($brokerId === $this->leader) {
            return PartitionRole::Leader;
        }

        if (\in_array($brokerId, $this->isr, true)) {
            return PartitionRole::InSyncReplica;
        }

        if (\in_array($brokerId, $this->replicas, true)) {
            return PartitionRole::OutOfSyncReplica;
        }

        return PartitionRole::NotReplica;
    }
}
