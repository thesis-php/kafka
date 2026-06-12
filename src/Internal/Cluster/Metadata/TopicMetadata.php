<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

use Ramsey\Uuid\UuidInterface;

/**
 * @internal
 */
final readonly class TopicMetadata
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

    /**
     * @return positive-int
     */
    public function partitions(): int
    {
        $n = \count($this->partitions);
        \assert($n > 0);

        return $n;
    }
}
