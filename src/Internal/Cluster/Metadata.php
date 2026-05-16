<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

/**
 * @internal
 */
final readonly class Metadata
{
    /**
     * @param array<int, Metadata\BrokerMetadata> $brokers keyed by broker id
     * @param int $controllerId -1 when no controller is elected right now
     * @param array<non-empty-string, Metadata\TopicMetadata> $topics keyed by topic name
     * @param int $loadedAtMs wall-clock ms at snapshot creation
     */
    public function __construct(
        public array $brokers,
        public int $controllerId,
        public array $topics,
        public int $loadedAtMs,
    ) {}
}
