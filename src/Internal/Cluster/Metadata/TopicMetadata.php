<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

use Ramsey\Uuid\UuidInterface;

/**
 * @internal
 */
final readonly class TopicMetadata implements \Countable
{
    /**
     * @param non-empty-string $name
     * @param array<int, PartitionMetadata> $partitions keyed by partition id
     */
    public function __construct(
        public string $name,
        public UuidInterface $topicId,
        public TopicKind $kind,
        public array $partitions,
    ) {}

    #[\Override]
    public function count(): int
    {
        return \count($this->partitions);
    }
}
