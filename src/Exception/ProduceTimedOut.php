<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\NetworkException;

/**
 * The broker did not answer a Produce request within
 * {@see \Thesis\Kafka\Producer\Config::$produceTimeout} plus
 * {@see \Thesis\Kafka\Config::$requestTimeout}, also on the last attempt.
 *
 * @api
 */
final class ProduceTimedOut extends NetworkException
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
    ) {
        parent::__construct("Produce request to partition '{$partition}' in topic '{$topic}' got no answer in time");
    }
}
