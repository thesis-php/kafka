<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use Testo\Assert;
use Testo\Test;

final readonly class ErrorCodeTest
{
    /**
     * The errors whose exception class extends RetriableException in
     * Errors.java of the Kafka Java client (trunk).
     */
    private const array RETRIABLE = [
        ErrorCode::CorruptMessage,
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::LeaderNotAvailable,
        ErrorCode::NotLeaderForPartition,
        ErrorCode::RequestTimedOut,
        ErrorCode::ReplicaNotAvailable,
        ErrorCode::NetworkException,
        ErrorCode::CoordinatorLoadInProgress,
        ErrorCode::CoordinatorNotAvailable,
        ErrorCode::NotCoordinator,
        ErrorCode::NotEnoughReplicas,
        ErrorCode::NotEnoughReplicasAfterAppend,
        ErrorCode::NotController,
        ErrorCode::ConcurrentTransactions,
        ErrorCode::KafkaStorageError,
        ErrorCode::FetchSessionIDNotFound,
        ErrorCode::InvalidFetchSessionEpoch,
        ErrorCode::ListenerNotFound,
        ErrorCode::FencedLeaderEpoch,
        ErrorCode::UnknownLeaderEpoch,
        ErrorCode::OffsetNotAvailable,
        ErrorCode::PreferredLeaderNotAvailable,
        ErrorCode::EligibleLeadersNotAvailable,
        ErrorCode::ElectionNotNeeded,
        ErrorCode::UnstableOffsetCommit,
        ErrorCode::ThrottlingQuotaExceeded,
        ErrorCode::UnknownTopicID,
        ErrorCode::InconsistentTopicID,
        ErrorCode::FetchSessionTopicIDError,
        ErrorCode::ShareSessionNotFound,
        ErrorCode::InvalidShareSessionEpoch,
        ErrorCode::ShareSessionLimitReached,
    ];

    #[Test]
    public function retriableErrorsAreTheSameAsInKafka(): void
    {
        $retriable = array_values(array_filter(
            ErrorCode::cases(),
            static fn(ErrorCode $code) => $code->retriable(),
        ));

        Assert::same(self::names($retriable), self::names(self::RETRIABLE));
    }

    /**
     * @param list<ErrorCode> $codes
     * @return list<string> names sorted by code
     */
    private static function names(array $codes): array
    {
        usort($codes, static fn(ErrorCode $a, ErrorCode $b) => $a->value <=> $b->value);

        return array_map(static fn(ErrorCode $code) => $code->name, $codes);
    }
}
