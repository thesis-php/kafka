<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 */
enum ErrorCode: int
{
    case None = 0;
    case UnknownServerError = -1;
    case OffsetOutOfRange = 1;
    case CorruptMessage = 2;
    case UnknownTopicOrPartition = 3;
    case InvalidFetchSize = 4;
    case LeaderNotAvailable = 5;
    case NotLeaderForPartition = 6;
    case RequestTimedOut = 7;
    case BrokerNotAvailable = 8;
    case ReplicaNotAvailable = 9;
    case MessageTooLarge = 10;
    case StaleControllerEpoch = 11;
    case OffsetMetadataTooLarge = 12;
    case NetworkException = 13;
    case CoordinatorLoadInProgress = 14;
    case CoordinatorNotAvailable = 15;
    case NotCoordinator = 16;
    case InvalidTopicException = 17;
    case RecordListTooLarge = 18;
    case NotEnoughReplicas = 19;
    case NotEnoughReplicasAfterAppend = 20;
    case InvalidRequiredAcks = 21;
    case IllegalGeneration = 22;
    case InconsistentGroupProtocol = 23;
    case InvalidGroupID = 24;
    case UnknownMemberID = 25;
    case InvalidSessionTimeout = 26;
    case RebalanceInProgress = 27;
    case InvalidCommitOffsetSize = 28;
    case TopicAuthorizationFailed = 29;
    case GroupAuthorizationFailed = 30;
    case ClusterAuthorizationFailed = 31;
    case InvalidTimestamp = 32;
    case UnsupportedSaslMechanism = 33;
    case IllegalSaslState = 34;
    case UnsupportedVersion = 35;
    case TopicAlreadyExists = 36;
    case InvalidPartitions = 37;
    case InvalidReplicationFactor = 38;
    case InvalidReplicaAssignment = 39;
    case InvalidConfig = 40;
    case NotController = 41;
    case InvalidRequest = 42;
    case UnsupportedForMessageFormat = 43;
    case PolicyViolation = 44;
    case OutOfOrderSequenceNumber = 45;
    case DuplicateSequenceNumber = 46;
    case InvalidProducerEpoch = 47;
    case InvalidTxnState = 48;
    case InvalidProducerIDMapping = 49;
    case InvalidTransactionTimeout = 50;
    case ConcurrentTransactions = 51;
    case TransactionCoordinatorFenced = 52;
    case TransactionalIDAuthorizationFailed = 53;
    case SecurityDisabled = 54;
    case OperationNotAttempted = 55;
    case KafkaStorageError = 56;
    case LogDirNotFound = 57;
    case SaslAuthenticationFailed = 58;
    case UnknownProducerID = 59;
    case ReassignmentInProgress = 60;
    case DelegationTokenAuthDisabled = 61;
    case DelegationTokenNotFound = 62;
    case DelegationTokenOwnerMismatch = 63;
    case DelegationTokenRequestNotAllowed = 64;
    case DelegationTokenAuthorizationFailed = 65;
    case DelegationTokenExpired = 66;
    case InvalidPrincipalType = 67;
    case NonEmptyGroup = 68;
    case GroupIDNotFound = 69;
    case FetchSessionIDNotFound = 70;
    case InvalidFetchSessionEpoch = 71;
    case ListenerNotFound = 72;
    case TopicDeletionDisabled = 73;
    case FencedLeaderEpoch = 74;
    case UnknownLeaderEpoch = 75;
    case UnsupportedCompressionType = 76;
    case StaleBrokerEpoch = 77;
    case OffsetNotAvailable = 78;
    case MemberIDRequired = 79;
    case PreferredLeaderNotAvailable = 80;
    case GroupMaxSizeReached = 81;
    case FencedInstanceID = 82;
    case EligibleLeadersNotAvailable = 83;
    case ElectionNotNeeded = 84;
    case NoReassignmentInProgress = 85;
    case GroupSubscribedToTopic = 86;
    case InvalidRecord = 87;
    case UnstableOffsetCommit = 88;
    case ThrottlingQuotaExceeded = 89;
    case ProducerFenced = 90;
    case ResourceNotFound = 91;
    case DuplicateResource = 92;
    case UnacceptableCredential = 93;
    case InconsistentVoterSet = 94;
    case InvalidUpdateVersion = 95;
    case FeatureUpdateFailed = 96;
    case PrincipalDeserializationFailure = 97;
    case SnapshotNotFound = 98;
    case PositionOutOfRange = 99;
    case UnknownTopicID = 100;
    case DuplicateBrokerRegistration = 101;
    case BrokerIDNotRegistered = 102;
    case InconsistentTopicID = 103;
    case InconsistentClusterID = 104;
    case TransactionalIDNotFound = 105;
    case FetchSessionTopicIDError = 106;
    case IneligibleReplica = 107;
    case NewLeaderElected = 108;
    case OffsetMovedToTieredStorage = 109;
    case FencedMemberEpoch = 110;
    case UnreleasedInstanceID = 111;
    case UnsupportedAssignor = 112;
    case StaleMemberEpoch = 113;
    case MismatchedEndpointType = 114;
    case UnsupportedEndpointType = 115;
    case UnknownControllerID = 116;
    case UnknownSubscriptionID = 117;
    case TelemetryTooLarge = 118;
    case InvalidRegistration = 119;
    case TransactionAbortable = 120;
    case InvalidRecordState = 121;
    case ShareSessionNotFound = 122;
    case InvalidShareSessionEpoch = 123;
    case FencedStateEpoch = 124;
    case InvalidVoterKey = 125;
    case DuplicateVoter = 126;
    case VoterNotFound = 127;
    case InvalidRegularExpression = 128;
    case RebootstrapRequired = 129;
    case StreamsInvalidTopology = 130;
    case StreamsInvalidTopologyEpoch = 131;
    case StreamsTopologyFenced = 132;
    case ShareSessionLimitReached = 133;

    public function message(): string
    {
        return match ($this) {
            self::None => 'NO_ERROR',
            self::UnknownServerError => 'UNKNOWN_SERVER_ERROR',
            self::OffsetOutOfRange => 'OFFSET_OUT_OF_RANGE',
            self::CorruptMessage => 'CORRUPT_MESSAGE',
            self::UnknownTopicOrPartition => 'UNKNOWN_TOPIC_OR_PARTITION',
            self::InvalidFetchSize => 'INVALID_FETCH_SIZE',
            self::LeaderNotAvailable => 'LEADER_NOT_AVAILABLE',
            self::NotLeaderForPartition => 'NOT_LEADER_FOR_PARTITION',
            self::RequestTimedOut => 'REQUEST_TIMED_OUT',
            self::BrokerNotAvailable => 'BROKER_NOT_AVAILABLE',
            self::ReplicaNotAvailable => 'REPLICA_NOT_AVAILABLE',
            self::MessageTooLarge => 'MESSAGE_TOO_LARGE',
            self::StaleControllerEpoch => 'STALE_CONTROLLER_EPOCH',
            self::OffsetMetadataTooLarge => 'OFFSET_METADATA_TOO_LARGE',
            self::NetworkException => 'NETWORK_EXCEPTION',
            self::CoordinatorLoadInProgress => 'COORDINATOR_LOAD_IN_PROGRESS',
            self::CoordinatorNotAvailable => 'COORDINATOR_NOT_AVAILABLE',
            self::NotCoordinator => 'NOT_COORDINATOR',
            self::InvalidTopicException => 'INVALID_TOPIC_EXCEPTION',
            self::RecordListTooLarge => 'RECORD_LIST_TOO_LARGE',
            self::NotEnoughReplicas => 'NOT_ENOUGH_REPLICAS',
            self::NotEnoughReplicasAfterAppend => 'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
            self::InvalidRequiredAcks => 'INVALID_REQUIRED_ACKS',
            self::IllegalGeneration => 'ILLEGAL_GENERATION',
            self::InconsistentGroupProtocol => 'INCONSISTENT_GROUP_PROTOCOL',
            self::InvalidGroupID => 'INVALID_GROUP_ID',
            self::UnknownMemberID => 'UNKNOWN_MEMBER_ID',
            self::InvalidSessionTimeout => 'INVALID_SESSION_TIMEOUT',
            self::RebalanceInProgress => 'REBALANCE_IN_PROGRESS',
            self::InvalidCommitOffsetSize => 'INVALID_COMMIT_OFFSET_SIZE',
            self::TopicAuthorizationFailed => 'TOPIC_AUTHORIZATION_FAILED',
            self::GroupAuthorizationFailed => 'GROUP_AUTHORIZATION_FAILED',
            self::ClusterAuthorizationFailed => 'CLUSTER_AUTHORIZATION_FAILED',
            self::InvalidTimestamp => 'INVALID_TIMESTAMP',
            self::UnsupportedSaslMechanism => 'UNSUPPORTED_SASL_MECHANISM',
            self::IllegalSaslState => 'ILLEGAL_SASL_STATE',
            self::UnsupportedVersion => 'UNSUPPORTED_VERSION',
            self::TopicAlreadyExists => 'TOPIC_ALREADY_EXISTS',
            self::InvalidPartitions => 'INVALID_PARTITIONS',
            self::InvalidReplicationFactor => 'INVALID_REPLICATION_FACTOR',
            self::InvalidReplicaAssignment => 'INVALID_REPLICA_ASSIGNMENT',
            self::InvalidConfig => 'INVALID_CONFIG',
            self::NotController => 'NOT_CONTROLLER',
            self::InvalidRequest => 'INVALID_REQUEST',
            self::UnsupportedForMessageFormat => 'UNSUPPORTED_FOR_MESSAGE_FORMAT',
            self::PolicyViolation => 'POLICY_VIOLATION',
            self::OutOfOrderSequenceNumber => 'OUT_OF_ORDER_SEQUENCE_NUMBER',
            self::DuplicateSequenceNumber => 'DUPLICATE_SEQUENCE_NUMBER',
            self::InvalidProducerEpoch => 'INVALID_PRODUCER_EPOCH',
            self::InvalidTxnState => 'INVALID_TXN_STATE',
            self::InvalidProducerIDMapping => 'INVALID_PRODUCER_ID_MAPPING',
            self::InvalidTransactionTimeout => 'INVALID_TRANSACTION_TIMEOUT',
            self::ConcurrentTransactions => 'CONCURRENT_TRANSACTIONS',
            self::TransactionCoordinatorFenced => 'TRANSACTION_COORDINATOR_FENCED',
            self::TransactionalIDAuthorizationFailed => 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
            self::SecurityDisabled => 'SECURITY_DISABLED',
            self::OperationNotAttempted => 'OPERATION_NOT_ATTEMPTED',
            self::KafkaStorageError => 'KAFKA_STORAGE_ERROR',
            self::LogDirNotFound => 'LOG_DIR_NOT_FOUND',
            self::SaslAuthenticationFailed => 'SASL_AUTHENTICATION_FAILED',
            self::UnknownProducerID => 'UNKNOWN_PRODUCER_ID',
            self::ReassignmentInProgress => 'REASSIGNMENT_IN_PROGRESS',
            self::DelegationTokenAuthDisabled => 'DELEGATION_TOKEN_AUTH_DISABLED',
            self::DelegationTokenNotFound => 'DELEGATION_TOKEN_NOT_FOUND',
            self::DelegationTokenOwnerMismatch => 'DELEGATION_TOKEN_OWNER_MISMATCH',
            self::DelegationTokenRequestNotAllowed => 'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED',
            self::DelegationTokenAuthorizationFailed => 'DELEGATION_TOKEN_AUTHORIZATION_FAILED',
            self::DelegationTokenExpired => 'DELEGATION_TOKEN_EXPIRED',
            self::InvalidPrincipalType => 'INVALID_PRINCIPAL_TYPE',
            self::NonEmptyGroup => 'NON_EMPTY_GROUP',
            self::GroupIDNotFound => 'GROUP_ID_NOT_FOUND',
            self::FetchSessionIDNotFound => 'FETCH_SESSION_ID_NOT_FOUND',
            self::InvalidFetchSessionEpoch => 'INVALID_FETCH_SESSION_EPOCH',
            self::ListenerNotFound => 'LISTENER_NOT_FOUND',
            self::TopicDeletionDisabled => 'TOPIC_DELETION_DISABLED',
            self::FencedLeaderEpoch => 'FENCED_LEADER_EPOCH',
            self::UnknownLeaderEpoch => 'UNKNOWN_LEADER_EPOCH',
            self::UnsupportedCompressionType => 'UNSUPPORTED_COMPRESSION_TYPE',
            self::StaleBrokerEpoch => 'STALE_BROKER_EPOCH',
            self::OffsetNotAvailable => 'OFFSET_NOT_AVAILABLE',
            self::MemberIDRequired => 'MEMBER_ID_REQUIRED',
            self::PreferredLeaderNotAvailable => 'PREFERRED_LEADER_NOT_AVAILABLE',
            self::GroupMaxSizeReached => 'GROUP_MAX_SIZE_REACHED',
            self::FencedInstanceID => 'FENCED_INSTANCE_ID',
            self::EligibleLeadersNotAvailable => 'ELIGIBLE_LEADERS_NOT_AVAILABLE',
            self::ElectionNotNeeded => 'ELECTION_NOT_NEEDED',
            self::NoReassignmentInProgress => 'NO_REASSIGNMENT_IN_PROGRESS',
            self::GroupSubscribedToTopic => 'GROUP_SUBSCRIBED_TO_TOPIC',
            self::InvalidRecord => 'INVALID_RECORD',
            self::UnstableOffsetCommit => 'UNSTABLE_OFFSET_COMMIT',
            self::ThrottlingQuotaExceeded => 'THROTTLING_QUOTA_EXCEEDED',
            self::ProducerFenced => 'PRODUCER_FENCED',
            self::ResourceNotFound => 'RESOURCE_NOT_FOUND',
            self::DuplicateResource => 'DUPLICATE_RESOURCE',
            self::UnacceptableCredential => 'UNACCEPTABLE_CREDENTIAL',
            self::InconsistentVoterSet => 'INCONSISTENT_VOTER_SET',
            self::InvalidUpdateVersion => 'INVALID_UPDATE_VERSION',
            self::FeatureUpdateFailed => 'FEATURE_UPDATE_FAILED',
            self::PrincipalDeserializationFailure => 'PRINCIPAL_DESERIALIZATION_FAILURE',
            self::SnapshotNotFound => 'SNAPSHOT_NOT_FOUND',
            self::PositionOutOfRange => 'POSITION_OUT_OF_RANGE',
            self::UnknownTopicID => 'UNKNOWN_TOPIC_ID',
            self::DuplicateBrokerRegistration => 'DUPLICATE_BROKER_REGISTRATION',
            self::BrokerIDNotRegistered => 'BROKER_ID_NOT_REGISTERED',
            self::InconsistentTopicID => 'INCONSISTENT_TOPIC_ID',
            self::InconsistentClusterID => 'INCONSISTENT_CLUSTER_ID',
            self::TransactionalIDNotFound => 'TRANSACTIONAL_ID_NOT_FOUND',
            self::FetchSessionTopicIDError => 'FETCH_SESSION_TOPIC_ID_ERROR',
            self::IneligibleReplica => 'INELIGIBLE_REPLICA',
            self::NewLeaderElected => 'NEW_LEADER_ELECTED',
            self::OffsetMovedToTieredStorage => 'OFFSET_MOVED_TO_TIERED_STORAGE',
            self::FencedMemberEpoch => 'FENCED_MEMBER_EPOCH',
            self::UnreleasedInstanceID => 'UNRELEASED_INSTANCE_ID',
            self::UnsupportedAssignor => 'UNSUPPORTED_ASSIGNOR',
            self::StaleMemberEpoch => 'STALE_MEMBER_EPOCH',
            self::MismatchedEndpointType => 'MISMATCHED_ENDPOINT_TYPE',
            self::UnsupportedEndpointType => 'UNSUPPORTED_ENDPOINT_TYPE',
            self::UnknownControllerID => 'UNKNOWN_CONTROLLER_ID',
            self::UnknownSubscriptionID => 'UNKNOWN_SUBSCRIPTION_ID',
            self::TelemetryTooLarge => 'TELEMETRY_TOO_LARGE',
            self::InvalidRegistration => 'INVALID_REGISTRATION',
            self::TransactionAbortable => 'TRANSACTION_ABORTABLE',
            self::InvalidRecordState => 'INVALID_RECORD_STATE',
            self::ShareSessionNotFound => 'SHARE_SESSION_NOT_FOUND',
            self::InvalidShareSessionEpoch => 'INVALID_SHARE_SESSION_EPOCH',
            self::FencedStateEpoch => 'FENCED_STATE_EPOCH',
            self::InvalidVoterKey => 'INVALID_VOTER_KEY',
            self::DuplicateVoter => 'DUPLICATE_VOTER',
            self::VoterNotFound => 'VOTER_NOT_FOUND',
            self::InvalidRegularExpression => 'INVALID_REGULAR_EXPRESSION',
            self::RebootstrapRequired => 'REBOOTSTRAP_REQUIRED',
            self::StreamsInvalidTopology => 'STREAMS_INVALID_TOPOLOGY',
            self::StreamsInvalidTopologyEpoch => 'STREAMS_INVALID_TOPOLOGY_EPOCH',
            self::StreamsTopologyFenced => 'STREAMS_TOPOLOGY_FENCED',
            self::ShareSessionLimitReached => 'SHARE_SESSION_LIMIT_REACHED',
        };
    }

    public function retriable(): bool
    {
        return match ($this) {
            self::CorruptMessage,
            self::UnknownTopicOrPartition,
            self::LeaderNotAvailable,
            self::NotLeaderForPartition,
            self::RequestTimedOut,
            self::BrokerNotAvailable,
            self::NetworkException,
            self::CoordinatorLoadInProgress,
            self::CoordinatorNotAvailable,
            self::NotEnoughReplicas,
            self::NotEnoughReplicasAfterAppend,
            self::NotController,
            self::KafkaStorageError,
            self::FetchSessionIDNotFound,
            self::InvalidFetchSessionEpoch,
            self::ListenerNotFound,
            self::FencedLeaderEpoch,
            self::UnknownLeaderEpoch,
            self::OffsetNotAvailable,
            self::EligibleLeadersNotAvailable,
            self::ElectionNotNeeded,
            self::UnstableOffsetCommit,
            self::ThrottlingQuotaExceeded,
            self::UnknownTopicID,
            self::FetchSessionTopicIDError,
            self::ShareSessionLimitReached => true,
            default => false,
        };
    }

    /**
     * @return non-empty-string
     */
    public function description(): string
    {
        return match ($this) {
            self::None => 'No error',
            self::UnknownServerError => 'The server experienced an unexpected error when processing the request.',
            self::OffsetOutOfRange => 'The requested offset is not within the range of offsets maintained by the server.',
            self::CorruptMessage => 'This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.',
            self::UnknownTopicOrPartition => 'This server does not host this topic-partition.',
            self::InvalidFetchSize => 'The requested fetch size is invalid.',
            self::LeaderNotAvailable => 'There is no leader for this topic-partition as we are in the middle of a leadership election.',
            self::NotLeaderForPartition => 'This server is not the leader for that topic-partition.',
            self::RequestTimedOut => 'The request timed out.',
            self::BrokerNotAvailable => 'The broker is not available.',
            self::ReplicaNotAvailable => 'The replica is not available for the requested topic-partition.',
            self::MessageTooLarge => 'The request included a message larger than the max message size the server will accept',
            self::StaleControllerEpoch => 'The controller moved to another broker.',
            self::OffsetMetadataTooLarge => 'The metadata field of the offset request was too large.',
            self::NetworkException => 'The server disconnected before a response was received.',
            self::CoordinatorLoadInProgress => 'The coordinator is loading and hence can\'t process requests.',
            self::CoordinatorNotAvailable => 'The coordinator is not available.',
            self::NotCoordinator => 'This is not the correct coordinator.',
            self::InvalidTopicException => 'The request attempted to perform an operation on an invalid topic.',
            self::RecordListTooLarge => 'The request included message batch larger than the configured segment size on the server.',
            self::NotEnoughReplicas => 'Messages are rejected since there are fewer in-sync replicas than required.',
            self::NotEnoughReplicasAfterAppend => 'Messages are written to the log, but to fewer in-sync replicas than required.',
            self::InvalidRequiredAcks => 'Produce request specified an invalid value for required acks.',
            self::IllegalGeneration => 'Specified group generation id is not valid.',
            self::InconsistentGroupProtocol => 'The group member\'s supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.',
            self::InvalidGroupID => 'The configured groupID is invalid.',
            self::UnknownMemberID => 'The coordinator is not aware of this member.',
            self::InvalidSessionTimeout => 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).',
            self::RebalanceInProgress => 'The group is rebalancing, so a rejoin is needed.',
            self::InvalidCommitOffsetSize => 'The committing offset data size is not valid.',
            self::TopicAuthorizationFailed => 'Not authorized to access topics: [Topic authorization failed.]',
            self::GroupAuthorizationFailed => 'Not authorized to access group: Group authorization failed.',
            self::ClusterAuthorizationFailed => 'Cluster authorization failed.',
            self::InvalidTimestamp => 'The timestamp of the message is out of acceptable range.',
            self::UnsupportedSaslMechanism => 'The broker does not support the requested SASL mechanism.',
            self::IllegalSaslState => 'Request is not valid given the current SASL state.',
            self::UnsupportedVersion => 'The version of API is not supported.',
            self::TopicAlreadyExists => 'Topic with this name already exists.',
            self::InvalidPartitions => 'Number of partitions is below 1.',
            self::InvalidReplicationFactor => 'Replication factor is below 1 or larger than the number of available brokers.',
            self::InvalidReplicaAssignment => 'Replica assignment is invalid.',
            self::InvalidConfig => 'Configuration is invalid.',
            self::NotController => 'This is not the correct controller for this cluster.',
            self::InvalidRequest => 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.',
            self::UnsupportedForMessageFormat => 'The message format version on the broker does not support the request.',
            self::PolicyViolation => 'Request parameters do not satisfy the configured policy.',
            self::OutOfOrderSequenceNumber => 'The broker received an out of order sequence number.',
            self::DuplicateSequenceNumber => 'The broker received a duplicate sequence number.',
            self::InvalidProducerEpoch => 'Producer attempted an operation with an old epoch.',
            self::InvalidTxnState => 'The producer attempted a transactional operation in an invalid state.',
            self::InvalidProducerIDMapping => 'The producer attempted to use a producer id which is not currently assigned to its transactional id.',
            self::InvalidTransactionTimeout => 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).',
            self::ConcurrentTransactions => 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.',
            self::TransactionCoordinatorFenced => 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.',
            self::TransactionalIDAuthorizationFailed => 'Transactional ID authorization failed.',
            self::SecurityDisabled => 'Security features are disabled.',
            self::OperationNotAttempted => 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.',
            self::KafkaStorageError => 'Disk error when trying to access log file on the disk.',
            self::LogDirNotFound => 'The user-specified log directory is not found in the broker config.',
            self::SaslAuthenticationFailed => 'SASL Authentication failed.',
            self::UnknownProducerID => 'This exception is raised by the broker if it could not locate the producer metadata associated with the producerID in question. This could happen if, for instance, the producer\'s records were deleted because their retention time had elapsed. Once the last records of the producerID are removed, the producer\'s metadata is removed from the broker, and future appends by the producer will return this exception.',
            self::ReassignmentInProgress => 'A partition reassignment is in progress.',
            self::DelegationTokenAuthDisabled => 'Delegation Token feature is not enabled.',
            self::DelegationTokenNotFound => 'Delegation Token is not found on server.',
            self::DelegationTokenOwnerMismatch => 'Specified Principal is not valid Owner/Renewer.',
            self::DelegationTokenRequestNotAllowed => 'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.',
            self::DelegationTokenAuthorizationFailed => 'Delegation Token authorization failed.',
            self::DelegationTokenExpired => 'Delegation Token is expired.',
            self::InvalidPrincipalType => 'Supplied principalType is not supported.',
            self::NonEmptyGroup => 'The group is not empty.',
            self::GroupIDNotFound => 'The group id does not exist.',
            self::FetchSessionIDNotFound => 'The fetch session ID was not found.',
            self::InvalidFetchSessionEpoch => 'The fetch session epoch is invalid.',
            self::ListenerNotFound => 'There is no listener on the leader broker that matches the listener on which metadata request was processed.',
            self::TopicDeletionDisabled => 'Topic deletion is disabled.',
            self::FencedLeaderEpoch => 'The leader epoch in the request is older than the epoch on the broker',
            self::UnknownLeaderEpoch => 'The leader epoch in the request is newer than the epoch on the broker',
            self::UnsupportedCompressionType => 'The requesting client does not support the compression type of given partition.',
            self::StaleBrokerEpoch => 'Broker epoch has changed',
            self::OffsetNotAvailable => 'The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing',
            self::MemberIDRequired => 'The group member needs to have a valid member id before actually entering a consumer group',
            self::PreferredLeaderNotAvailable => 'The preferred leader was not available',
            self::GroupMaxSizeReached => 'The consumer group has reached its max size',
            self::FencedInstanceID => 'The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.',
            self::EligibleLeadersNotAvailable => 'Eligible topic partition leaders are not available',
            self::ElectionNotNeeded => 'Leader election not needed for topic partition',
            self::NoReassignmentInProgress => 'No partition reassignment is in progress.',
            self::GroupSubscribedToTopic => 'Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.',
            self::InvalidRecord => 'This record has failed the validation on the broker and hence been rejected.',
            self::UnstableOffsetCommit => 'There are unstable offsets that need to be cleared.',
            self::ThrottlingQuotaExceeded => 'The throttling quota has been exceeded.',
            self::ProducerFenced => 'There is a newer producer with the same transactionalId which fences the current one.',
            self::ResourceNotFound => 'A request illegally referred to a resource that does not exist.',
            self::DuplicateResource => 'A request illegally referred to the same resource twice.',
            self::UnacceptableCredential => 'Requested credential would not meet criteria for acceptability.',
            self::InconsistentVoterSet => 'Indicates that either the sender or recipient of a voter-only request is not one of the expected voters.',
            self::InvalidUpdateVersion => 'The given update version was invalid.',
            self::FeatureUpdateFailed => 'Unable to update finalized features due to an unexpected server error.',
            self::PrincipalDeserializationFailure => 'Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.',
            self::SnapshotNotFound => 'Requested snapshot was not found.',
            self::PositionOutOfRange => 'Requested position is not greater than or equal to zero, and less than the size of the snapshot.',
            self::UnknownTopicID => 'This server does not host this topic ID.',
            self::DuplicateBrokerRegistration => 'This broker ID is already in use.',
            self::BrokerIDNotRegistered => 'The given broker ID was not registered.',
            self::InconsistentTopicID => 'The log\'s topic ID did not match the topic ID in the request.',
            self::InconsistentClusterID => 'The clusterId in the request does not match that found on the server.',
            self::TransactionalIDNotFound => 'The transactionalId could not be found.',
            self::FetchSessionTopicIDError => 'The fetch session encountered inconsistent topic ID usage.',
            self::IneligibleReplica => 'The new ISR contains at least one ineligible replica.',
            self::NewLeaderElected => 'The AlterPartition request successfully updated the partition state but the leader has changed.',
            self::OffsetMovedToTieredStorage => 'The requested offset is moved to tiered storage.',
            self::FencedMemberEpoch => 'The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.',
            self::UnreleasedInstanceID => 'The instance ID is still used by another member in the consumer group. That member must leave first.',
            self::UnsupportedAssignor => 'The assignor or its version range is not supported by the consumer group.',
            self::StaleMemberEpoch => 'The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.',
            self::MismatchedEndpointType => 'The request was sent to an endpoint of the wrong type.',
            self::UnsupportedEndpointType => 'This endpoint type is not supported yet.',
            self::UnknownControllerID => 'This controller ID is not known',
            self::UnknownSubscriptionID => 'Client sent a push telemetry request with an invalid or outdated subscription ID.',
            self::TelemetryTooLarge => 'Client sent a push telemetry request larger than the maximum size the broker will accept.',
            self::InvalidRegistration => 'The controller has considered the broker registration to be invalid.',
            self::TransactionAbortable => 'The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.',
            self::InvalidRecordState => 'The record state is invalid. The acknowledgement of delivery could not be completed.',
            self::ShareSessionNotFound => 'The share session was not found.',
            self::InvalidShareSessionEpoch => 'The share session epoch is invalid.',
            self::FencedStateEpoch => 'The share coordinator rejected the request because the share-group state epoch did not match.',
            self::InvalidVoterKey => 'The voter key doesn\'t match the receiving replica\'s key.',
            self::DuplicateVoter => 'The voter is already part of the set of voters.',
            self::VoterNotFound => 'The voter is not part of the set of voters.',
            self::InvalidRegularExpression => 'The regular expression is not valid.',
            self::RebootstrapRequired => 'Client metadata is stale. The client should rebootstrap to obtain new metadata.',
            self::StreamsInvalidTopology => 'The supplied topology is invalid.',
            self::StreamsInvalidTopologyEpoch => 'The supplied topology epoch is invalid.',
            self::StreamsTopologyFenced => 'The supplied topology epoch is outdated.',
            self::ShareSessionLimitReached => 'The limit of share sessions has been reached.',
        };
    }
}
