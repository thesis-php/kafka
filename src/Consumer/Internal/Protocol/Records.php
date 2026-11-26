<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\Protocol;

use Thesis\Kafka\Consumer;
use Thesis\Kafka\Internal\Protocol\Record;

/**
 * @internal
 */
final class Records
{
    /** @var positive-int */
    public int $nextOffset {
        get => $this->firstOffset + $this->lastOffsetDelta + 1; // @phpstan-ignore return.type
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param list<Consumer\Record> $records
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $firstOffset,
        public readonly int $length,
        public readonly int $partitionLeaderEpoch,
        public readonly Record\Attributes $attributes,
        public readonly int $lastOffsetDelta,
        public readonly int $firstTimestamp,
        public readonly int $maxTimestamp,
        public readonly int $producerId,
        public readonly int $producerEpoch,
        public readonly int $baseSequence,
        public private(set) array $records = [],
    ) {}

    /**
     * @no-named-arguments
     */
    public function add(Consumer\Record ...$records): void
    {
        foreach ($records as $record) {
            $this->records[] = $record;
        }
    }
}
