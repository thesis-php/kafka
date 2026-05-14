<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * The Producer guarantees exactly one instance per (Partitioner, topic)
 * pair, and never invokes its methods concurrently. Implementations
 * may therefore keep state in plain properties.
 *
 * @api
 */
interface TopicPartitioner
{
    /**
     * @param positive-int $partitions number of partitions currently available
     * @return non-negative-int chosen index in [0, $partitions - 1]
     */
    public function partition(ProducerRecord $record, int $partitions): int;
}
