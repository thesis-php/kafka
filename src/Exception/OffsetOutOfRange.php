<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;
use Thesis\Kafka\TopicPartition;

/**
 * The broker has no records at the position of the consumer, and the
 * consumer has {@see \Thesis\Kafka\Consumer\AutoOffsetReset::None}.
 *
 * @api
 */
final class OffsetOutOfRange extends Exception
{
    /**
     * @param non-negative-int $offset the position that the broker rejected
     */
    public function __construct(
        public readonly TopicPartition $partition,
        public readonly int $offset,
    ) {
        parent::__construct("Offset {$offset} is out of range for {$partition} and auto.offset.reset is None.");
    }
}
