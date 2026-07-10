<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

/**
 * An offset with the leader epoch we read it under. We commit both. The broker
 * uses the epoch to detect log truncation (KIP-320): it fences a fetch or commit
 * that carries a stale epoch.
 *
 * @internal
 */
final readonly class EpochOffset
{
    /**
     * @param int $epoch leader epoch the offset was read under, -1 if unknown
     * @param non-negative-int $offset the next offset to read
     */
    public function __construct(
        public int $epoch,
        public int $offset,
    ) {}
}
