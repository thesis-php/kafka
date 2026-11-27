<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

/**
 * The idempotent numbers that one batch carries on every attempt.
 *
 * @internal
 */
final readonly class BatchSequence
{
    /**
     * @param non-negative-int $producerId
     * @param non-negative-int $producerEpoch
     * @param non-negative-int $baseSequence the sequence of the first record of the batch
     */
    public function __construct(
        public int $producerId,
        public int $producerEpoch,
        public int $baseSequence,
    ) {}
}
