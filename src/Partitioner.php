<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
interface Partitioner
{
    /**
     * @param positive-int $partitionCount
     * @return non-negative-int
     */
    public function partition(ProducerRecord $record, int $partitionCount): int;
}
