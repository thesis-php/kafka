<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

/**
 * @internal
 */
final readonly class NodeId implements \Stringable
{
    private const int UNKNOWN_ID_THRESHOLD = PHP_INT_MIN;

    public function __construct(
        public int $id,
    ) {}

    public static function unknown(int $index): self
    {
        return new self(self::UNKNOWN_ID_THRESHOLD + $index);
    }

    #[\Override]
    public function __toString(): string
    {
        // Real broker IDs assigned by Kafka are non-negative. The value
        // -1 is reserved by the MetadataResponse protocol to mean
        // "unknown controller" — when the cluster has no elected
        // controller at the moment. Anything below -1 cannot come from
        // a real broker and therefore identifies a seed: an endpoint
        // we got from Config before any metadata was loaded.
        return match (true) {
            $this->id < -1 => "seed_{$this->id}",
            $this->id === -1 => 'unknown_controller',
            default => "known_{$this->id}",
        };
    }
}
