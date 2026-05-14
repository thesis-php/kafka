<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Random\Randomizer;
use Thesis\Kafka\Partitioner;
use Thesis\Kafka\ProducerRecord;

/**
 * @api
 */
final readonly class RandomPartitioner implements Partitioner
{
    public function __construct(
        private Randomizer $randomizer = new Randomizer(),
    ) {}

    #[\Override]
    public function partition(ProducerRecord $record, int $partitionCount): int
    {
        /** @var non-negative-int */
        return $this->randomizer->getInt(0, $partitionCount - 1);
    }
}
