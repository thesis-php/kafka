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

    public function clientVersionRange(): VersionRange
    {
        return match ($this) {
            self::Produce => new VersionRange(3, 13),
            self::Fetch => new VersionRange(4, 18),
            self::ListOffsets => new VersionRange(1, 11),
            self::Metadata => new VersionRange(0, 13),
            self::OffsetCommit => new VersionRange(2, 10),
            self::OffsetFetch => new VersionRange(1, 10),
            self::FindCoordinator => new VersionRange(0, 6),
            self::JoinGroup => new VersionRange(0, 9),
            self::Heartbeat => new VersionRange(0, 4),
            self::LeaveGroup => new VersionRange(0, 5),
            self::SyncGroup => new VersionRange(0, 5),
            self::DescribeGroups => new VersionRange(0, 6),
            self::ListGroups => new VersionRange(0, 5),
            self::SaslHandshake => new VersionRange(0, 1),
            self::ApiVersions => new VersionRange(0, 4),
            self::CreateTopics => new VersionRange(2, 7),
            self::DeleteTopics => new VersionRange(1, 6),
            self::DeleteRecords => new VersionRange(0, 2),
            self::InitProducerId => new VersionRange(0, 6),
            self::OffsetForLeaderEpoch => new VersionRange(2, 4),
            self::AddPartitionsToTxn => new VersionRange(0, 5),
            self::AddOffsetsToTxn => new VersionRange(0, 4),
            self::EndTxn => new VersionRange(0, 5),
            self::WriteTxnMarkers => new VersionRange(1, 2),
            self::TxnOffsetCommit => new VersionRange(0, 5),
            self::DescribeAcls => new VersionRange(1, 3),
            self::CreateAcls => new VersionRange(1, 3),
            self::DeleteAcls => new VersionRange(1, 3),
            self::DescribeConfigs => new VersionRange(1, 4),
            self::AlterConfigs => new VersionRange(0, 2),
            self::AlterReplicaLogDirs => new VersionRange(1, 2),
            self::DescribeLogDirs => new VersionRange(1, 4),
            self::SaslAuthenticate => new VersionRange(0, 2),
            self::CreatePartitions => new VersionRange(0, 3),
            self::CreateDelegationToken => new VersionRange(1, 3),
            self::RenewDelegationToken => new VersionRange(1, 2),
            self::ExpireDelegationToken => new VersionRange(1, 2),
            self::DescribeDelegationToken => new VersionRange(1, 3),
            self::DeleteGroups => new VersionRange(0, 2),
            self::ElectLeaders => new VersionRange(0, 2),
            self::IncrementalAlterConfigs => new VersionRange(0, 1),
            self::AlterPartitionReassignments => new VersionRange(0, 1),
            self::ListPartitionReassignments => new VersionRange(0, 0),
            self::OffsetDelete => new VersionRange(0, 0),
            self::DescribeClientQuotas => new VersionRange(0, 1),
            self::AlterClientQuotas => new VersionRange(0, 1),
            self::DescribeUserScramCredentials => new VersionRange(0, 0),
            self::AlterUserScramCredentials => new VersionRange(0, 0),
            self::Vote => new VersionRange(0, 2),
            self::BeginQuorumEpoch => new VersionRange(0, 1),
            self::EndQuorumEpoch => new VersionRange(0, 1),
            self::DescribeQuorum => new VersionRange(0, 2),
            self::AlterPartition => new VersionRange(2, 3),
            self::UpdateFeatures => new VersionRange(0, 2),
            self::Envelope => new VersionRange(0, 0),
            self::FetchSnapshot => new VersionRange(0, 1),
            self::DescribeCluster => new VersionRange(0, 2),
            self::DescribeProducers => new VersionRange(0, 0),
            self::BrokerRegistration => new VersionRange(0, 4),
            self::BrokerHeartbeat => new VersionRange(0, 1),
            self::UnregisterBroker => new VersionRange(0, 0),
            self::DescribeTransactions => new VersionRange(0, 0),
            self::ListTransactions => new VersionRange(0, 2),
            self::AllocateProducerIds => new VersionRange(0, 0),
            self::ConsumerGroupHeartbeat => new VersionRange(0, 1),
            self::ConsumerGroupDescribe => new VersionRange(0, 1),
            self::ControllerRegistration => new VersionRange(0, 0),
            self::GetTelemetrySubscriptions => new VersionRange(0, 0),
            self::PushTelemetry => new VersionRange(0, 0),
            self::AssignReplicasToDirs => new VersionRange(0, 0),
            self::ListConfigResources => new VersionRange(0, 1),
            self::DescribeTopicPartitions => new VersionRange(0, 0),
            self::ShareGroupHeartbeat => new VersionRange(1, 1),
            self::ShareGroupDescribe => new VersionRange(1, 1),
            self::ShareFetch => new VersionRange(1, 2),
            self::ShareAcknowledge => new VersionRange(1, 2),
            self::AddRaftVoter => new VersionRange(0, 1),
            self::RemoveRaftVoter => new VersionRange(0, 0),
            self::UpdateRaftVoter => new VersionRange(0, 0),
            self::InitializeShareGroupState => new VersionRange(0, 0),
            self::ReadShareGroupState => new VersionRange(0, 0),
            self::WriteShareGroupState => new VersionRange(0, 1),
            self::DeleteShareGroupState => new VersionRange(0, 0),
            self::ReadShareGroupStateSummary => new VersionRange(0, 1),
            self::StreamsGroupHeartbeat => new VersionRange(0, 0),
            self::StreamsGroupDescribe => new VersionRange(0, 0),
            self::DescribeShareGroupOffsets => new VersionRange(0, 1),
            self::AlterShareGroupOffsets => new VersionRange(0, 0),
            self::DeleteShareGroupOffsets => new VersionRange(0, 0),
        };
    }
}
