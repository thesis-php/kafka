<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;
use Thesis\Kafka\TopicPartition;

/**
 * @api
 */
final class NoOffsetForPartitions extends Exception
{
    /**
     * @param non-empty-list<TopicPartition> $partitions
     */
    public function __construct(
        public readonly array $partitions,
    ) {
        $formatted = implode(', ', array_map(
            static fn(TopicPartition $tp) => (string) $tp,
            $partitions,
        ));

        parent::__construct("No committed offset for {$formatted} and auto.offset.reset is None.");
    }
}
