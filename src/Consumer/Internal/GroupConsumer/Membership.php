<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final readonly class Membership
{
    /**
     * @param string $memberId The id the coordinator assigned this member;
     *        stable for the generation, reused as input to the next join.
     * @param int $generationId The generation this membership belongs to.
     *        Heartbeats and commits carrying a stale generation are rejected.
     * @param list<TopicPartition> $assignments
     */
    public function __construct(
        public string $memberId,
        public int $generationId,
        public ?string $protocolName,
        public array $assignments,
    ) {}
}
