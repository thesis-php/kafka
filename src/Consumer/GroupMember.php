<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

/**
 * A group member as the balancer sees it during assignment: its
 * coordinator-assigned id and the decoded contents of its JoinGroup
 * subscription. Built by the leader from each JoinGroupResponseMember by
 * decoding the embedded ConsumerProtocolSubscription bytes.
 *
 * @api
 */
final readonly class GroupMember
{
    /**
     * @param non-empty-string $memberId
     * @param non-empty-list<non-empty-string> $topics topics this member subscribed to
     * @param array<non-empty-string, list<int>> $ownedPartitions currently owned (cooperative), empty for eager
     * @param ?string $userData opaque assignor state (sticky), null otherwise
     */
    public function __construct(
        public string $memberId,
        public array $topics,
        public array $ownedPartitions = [],
        public ?string $userData = null,
    ) {}
}
