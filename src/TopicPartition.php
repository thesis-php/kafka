<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * Identifies a single partition of a topic. A value object used as the
 * unit of assignment, commit, seek, and position tracking.
 *
 * @api
 */
final readonly class TopicPartition implements \Stringable
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     */
    public function __construct(
        public string $topic,
        public int $partition,
    ) {}

    /**
     * @return non-empty-string
     */
    public function key(): string
    {
        return "{$this->topic}-{$this->partition}";
    }

    /**
     * @return non-empty-string
     */
    #[\Override]
    public function __toString(): string
    {
        return $this->key();
    }
}
