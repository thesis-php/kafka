<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class UnknownPartition extends Exception
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
    ) {
        parent::__construct("Partition '{$partition}' does not exist in topic '{$topic}'");
    }
}
