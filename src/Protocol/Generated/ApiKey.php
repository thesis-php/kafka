<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\VersionRange;

/**
 * @api
 * @generated
 */
enum ApiKey: int
{
    case Produce = 0;
    case Fetch = 1;
    case ListOffsets = 2;
    case Metadata = 3;
    case OffsetCommit = 8;
    case OffsetFetch = 9;
    case FindCoordinator = 10;
    case JoinGroup = 11;
    case Heartbeat = 12;
    case LeaveGroup = 13;
    case SyncGroup = 14;
    case DescribeGroups = 15;
    case ListGroups = 16;
    case SaslHandshake = 17;
    case ApiVersions = 18;
    case CreateTopics = 19;
    case DeleteTopics = 20;
    case DeleteRecords = 21;
    case InitProducerId = 22;
    case OffsetForLeaderEpoch = 23;
    case AddPartitionsToTxn = 24;
    case AddOffsetsToTxn = 25;
    case EndTxn = 26;
    case WriteTxnMarkers = 27;
    case TxnOffsetCommit = 28;
    case DescribeAcls = 29;
    case CreateAcls = 30;
    case DeleteAcls = 31;
    case DescribeConfigs = 32;
    case AlterConfigs = 33;
    case AlterReplicaLogDirs = 34;
    case DescribeLogDirs = 35;
    case SaslAuthenticate = 36;
    case CreatePartitions = 37;
    case CreateDelegationToken = 38;
    case RenewDelegationToken = 39;
    case ExpireDelegationToken = 40;
    case DescribeDelegationToken = 41;
    case DeleteGroups = 42;
    case ElectLeaders = 43;
    case IncrementalAlterConfigs = 44;
    case AlterPartitionReassignments = 45;
    case ListPartitionReassignments = 46;
    case OffsetDelete = 47;
    case DescribeClientQuotas = 48;
    case AlterClientQuotas = 49;
    case DescribeUserScramCredentials = 50;
    case AlterUserScramCredentials = 51;
    case Vote = 52;
    case BeginQuorumEpoch = 53;
    case EndQuorumEpoch = 54;
    case DescribeQuorum = 55;
    case AlterPartition = 56;
    case UpdateFeatures = 57;
    case Envelope = 58;
    case FetchSnapshot = 59;
    case DescribeCluster = 60;
    case DescribeProducers = 61;
    case BrokerRegistration = 62;
    case BrokerHeartbeat = 63;
    case UnregisterBroker = 64;
    case DescribeTransactions = 65;
    case ListTransactions = 66;
    case AllocateProducerIds = 67;
    case ConsumerGroupHeartbeat = 68;
    case ConsumerGroupDescribe = 69;
    case ControllerRegistration = 70;
    case GetTelemetrySubscriptions = 71;
    case PushTelemetry = 72;
    case AssignReplicasToDirs = 73;
    case ListConfigResources = 74;
    case DescribeTopicPartitions = 75;
    case ShareGroupHeartbeat = 76;
    case ShareGroupDescribe = 77;
    case ShareFetch = 78;
    case ShareAcknowledge = 79;
    case AddRaftVoter = 80;
    case RemoveRaftVoter = 81;
    case UpdateRaftVoter = 82;
    case InitializeShareGroupState = 83;
    case ReadShareGroupState = 84;
    case WriteShareGroupState = 85;
    case DeleteShareGroupState = 86;
    case ReadShareGroupStateSummary = 87;
    case StreamsGroupHeartbeat = 88;
    case StreamsGroupDescribe = 89;
    case DescribeShareGroupOffsets = 90;
    case AlterShareGroupOffsets = 91;
    case DeleteShareGroupOffsets = 92;

    /**
     * @return list<Listener>
     */
    public function listeners(): array
    {
        return match ($this) {
            self::Produce,
            self::ListOffsets,
            self::Metadata,
            self::OffsetCommit,
            self::OffsetFetch,
            self::FindCoordinator,
            self::JoinGroup,
            self::Heartbeat,
            self::LeaveGroup,
            self::SyncGroup,
            self::DescribeGroups,
            self::ListGroups,
            self::DeleteRecords,
            self::InitProducerId,
            self::OffsetForLeaderEpoch,
            self::AddPartitionsToTxn,
            self::AddOffsetsToTxn,
            self::EndTxn,
            self::WriteTxnMarkers,
            self::TxnOffsetCommit,
            self::AlterReplicaLogDirs,
            self::DescribeLogDirs,
            self::DeleteGroups,
            self::OffsetDelete,
            self::DescribeClientQuotas,
            self::DescribeProducers,
            self::DescribeTransactions,
            self::ListTransactions,
            self::ConsumerGroupHeartbeat,
            self::ConsumerGroupDescribe,
            self::GetTelemetrySubscriptions,
            self::PushTelemetry,
            self::ListConfigResources,
            self::DescribeTopicPartitions,
            self::ShareGroupHeartbeat,
            self::ShareGroupDescribe,
            self::ShareFetch,
            self::ShareAcknowledge,
            self::InitializeShareGroupState,
            self::ReadShareGroupState,
            self::WriteShareGroupState,
            self::DeleteShareGroupState,
            self::ReadShareGroupStateSummary,
            self::StreamsGroupHeartbeat,
            self::StreamsGroupDescribe,
            self::DescribeShareGroupOffsets,
            self::AlterShareGroupOffsets,
            self::DeleteShareGroupOffsets => [Listener::Broker],
            self::Fetch,
            self::SaslHandshake,
            self::ApiVersions,
            self::CreateTopics,
            self::DeleteTopics,
            self::DescribeAcls,
            self::CreateAcls,
            self::DeleteAcls,
            self::DescribeConfigs,
            self::AlterConfigs,
            self::SaslAuthenticate,
            self::CreatePartitions,
            self::CreateDelegationToken,
            self::RenewDelegationToken,
            self::ExpireDelegationToken,
            self::DescribeDelegationToken,
            self::ElectLeaders,
            self::IncrementalAlterConfigs,
            self::AlterPartitionReassignments,
            self::ListPartitionReassignments,
            self::AlterClientQuotas,
            self::DescribeUserScramCredentials,
            self::AlterUserScramCredentials,
            self::DescribeQuorum,
            self::UpdateFeatures,
            self::DescribeCluster,
            self::UnregisterBroker,
            self::AddRaftVoter,
            self::RemoveRaftVoter => [Listener::Broker, Listener::Controller],
            self::Vote,
            self::BeginQuorumEpoch,
            self::EndQuorumEpoch,
            self::AlterPartition,
            self::Envelope,
            self::FetchSnapshot,
            self::BrokerRegistration,
            self::BrokerHeartbeat,
            self::AllocateProducerIds,
            self::ControllerRegistration,
            self::AssignReplicasToDirs,
            self::UpdateRaftVoter => [Listener::Controller],
        };
    }

    public function requiresAnyListener(Listener ...$listeners): bool
    {
        return array_any($listeners, fn(Listener $listener) => \in_array($listener, $this->listeners(), true));
    }

    public function clientVersionRange(): VersionRange
    {
        return match ($this) {
            self::Produce => new VersionRange(3, 13),
            self::Fetch => new VersionRange(4, 18),
            self::ListOffsets => new VersionRange(1, 11),
            self::Metadata => new VersionRange(0, 13),
            self::OffsetCommit => new VersionRange(2, 10),
            self::OffsetFetch => new VersionRange(1, 10),
            self::FindCoordinator,
            self::DescribeGroups,
            self::InitProducerId => new VersionRange(0, 6),
            self::JoinGroup => new VersionRange(0, 9),
            self::Heartbeat,
            self::ApiVersions,
            self::AddOffsetsToTxn,
            self::BrokerRegistration => new VersionRange(0, 4),
            self::LeaveGroup,
            self::SyncGroup,
            self::ListGroups,
            self::AddPartitionsToTxn,
            self::EndTxn,
            self::TxnOffsetCommit => new VersionRange(0, 5),
            self::SaslHandshake,
            self::IncrementalAlterConfigs,
            self::AlterPartitionReassignments,
            self::DescribeClientQuotas,
            self::AlterClientQuotas,
            self::BeginQuorumEpoch,
            self::EndQuorumEpoch,
            self::FetchSnapshot,
            self::BrokerHeartbeat,
            self::ConsumerGroupHeartbeat,
            self::ConsumerGroupDescribe,
            self::ListConfigResources,
            self::AddRaftVoter,
            self::WriteShareGroupState,
            self::ReadShareGroupStateSummary,
            self::DescribeShareGroupOffsets => new VersionRange(0, 1),
            self::CreateTopics => new VersionRange(2, 7),
            self::DeleteTopics => new VersionRange(1, 6),
            self::DeleteRecords,
            self::AlterConfigs,
            self::SaslAuthenticate,
            self::DeleteGroups,
            self::ElectLeaders,
            self::Vote,
            self::DescribeQuorum,
            self::UpdateFeatures,
            self::DescribeCluster,
            self::ListTransactions => new VersionRange(0, 2),
            self::OffsetForLeaderEpoch => new VersionRange(2, 4),
            self::WriteTxnMarkers,
            self::AlterReplicaLogDirs,
            self::RenewDelegationToken,
            self::ExpireDelegationToken,
            self::ShareFetch,
            self::ShareAcknowledge => new VersionRange(1, 2),
            self::DescribeAcls,
            self::CreateAcls,
            self::DeleteAcls,
            self::CreateDelegationToken,
            self::DescribeDelegationToken => new VersionRange(1, 3),
            self::DescribeConfigs,
            self::DescribeLogDirs => new VersionRange(1, 4),
            self::CreatePartitions => new VersionRange(0, 3),
            self::ListPartitionReassignments,
            self::OffsetDelete,
            self::DescribeUserScramCredentials,
            self::AlterUserScramCredentials,
            self::Envelope,
            self::DescribeProducers,
            self::UnregisterBroker,
            self::DescribeTransactions,
            self::AllocateProducerIds,
            self::ControllerRegistration,
            self::GetTelemetrySubscriptions,
            self::PushTelemetry,
            self::AssignReplicasToDirs,
            self::DescribeTopicPartitions,
            self::RemoveRaftVoter,
            self::UpdateRaftVoter,
            self::InitializeShareGroupState,
            self::ReadShareGroupState,
            self::DeleteShareGroupState,
            self::StreamsGroupHeartbeat,
            self::StreamsGroupDescribe,
            self::AlterShareGroupOffsets,
            self::DeleteShareGroupOffsets => new VersionRange(0, 0),
            self::AlterPartition => new VersionRange(2, 3),
            self::ShareGroupHeartbeat,
            self::ShareGroupDescribe => new VersionRange(1, 1),
        };
    }

    public function isFlexibleVersion(int $apiVersion): bool
    {
        return match ($this) {
            self::Produce,
            self::Metadata => $apiVersion >= 9,
            self::Fetch => $apiVersion >= 12,
            self::ListOffsets,
            self::OffsetFetch,
            self::JoinGroup => $apiVersion >= 6,
            self::OffsetCommit => $apiVersion >= 8,
            self::FindCoordinator,
            self::ListGroups,
            self::ApiVersions,
            self::AddPartitionsToTxn,
            self::AddOffsetsToTxn,
            self::EndTxn,
            self::TxnOffsetCommit => $apiVersion >= 3,
            self::Heartbeat,
            self::LeaveGroup,
            self::SyncGroup,
            self::DeleteTopics,
            self::OffsetForLeaderEpoch,
            self::DescribeConfigs => $apiVersion >= 4,
            self::DescribeGroups,
            self::CreateTopics => $apiVersion >= 5,
            self::SaslHandshake,
            self::OffsetDelete => false,
            self::DeleteRecords,
            self::InitProducerId,
            self::DescribeAcls,
            self::CreateAcls,
            self::DeleteAcls,
            self::AlterConfigs,
            self::AlterReplicaLogDirs,
            self::DescribeLogDirs,
            self::SaslAuthenticate,
            self::CreatePartitions,
            self::CreateDelegationToken,
            self::RenewDelegationToken,
            self::ExpireDelegationToken,
            self::DescribeDelegationToken,
            self::DeleteGroups,
            self::ElectLeaders => $apiVersion >= 2,
            self::WriteTxnMarkers,
            self::IncrementalAlterConfigs,
            self::DescribeClientQuotas,
            self::AlterClientQuotas,
            self::BeginQuorumEpoch,
            self::EndQuorumEpoch => $apiVersion >= 1,
            self::AlterPartitionReassignments,
            self::ListPartitionReassignments,
            self::DescribeUserScramCredentials,
            self::AlterUserScramCredentials,
            self::Vote,
            self::DescribeQuorum,
            self::AlterPartition,
            self::UpdateFeatures,
            self::Envelope,
            self::FetchSnapshot,
            self::DescribeCluster,
            self::DescribeProducers,
            self::BrokerRegistration,
            self::BrokerHeartbeat,
            self::UnregisterBroker,
            self::DescribeTransactions,
            self::ListTransactions,
            self::AllocateProducerIds,
            self::ConsumerGroupHeartbeat,
            self::ConsumerGroupDescribe,
            self::ControllerRegistration,
            self::GetTelemetrySubscriptions,
            self::PushTelemetry,
            self::AssignReplicasToDirs,
            self::ListConfigResources,
            self::DescribeTopicPartitions,
            self::ShareGroupHeartbeat,
            self::ShareGroupDescribe,
            self::ShareFetch,
            self::ShareAcknowledge,
            self::AddRaftVoter,
            self::RemoveRaftVoter,
            self::UpdateRaftVoter,
            self::InitializeShareGroupState,
            self::ReadShareGroupState,
            self::WriteShareGroupState,
            self::DeleteShareGroupState,
            self::ReadShareGroupStateSummary,
            self::StreamsGroupHeartbeat,
            self::StreamsGroupDescribe,
            self::DescribeShareGroupOffsets,
            self::AlterShareGroupOffsets,
            self::DeleteShareGroupOffsets => $apiVersion >= 0,
        };
    }

    /**
     * @return non-negative-int
     */
    public function requestHeaderVersion(int $apiVersion): int
    {
        return $this->isFlexibleVersion($apiVersion) ? 2 : 1;
    }

    /**
     * @return non-negative-int
     */
    public function responseHeaderVersion(int $apiVersion): int
    {
        return match ($this) {
            self::ApiVersions => 0,
            default => $this->isFlexibleVersion($apiVersion) ? 1 : 0,
        };
    }
}
