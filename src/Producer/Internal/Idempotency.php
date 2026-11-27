<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

/**
 * The producer's idempotent identity: the broker-assigned id and the
 * newest epoch. A batch gets its numbers as a {@see BatchSequence}.
 *
 * @internal
 */
final readonly class Idempotency
{
    /**
     * @param non-negative-int $producerId
     * @param non-negative-int $producerEpoch
     */
    public function __construct(
        public int $producerId,
        public int $producerEpoch,
    ) {}
}
