<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class UnknownTopic extends Exception
{
    /**
     * @param non-empty-string $topic
     */
    public function __construct(
        public readonly string $topic,
    ) {
        parent::__construct("Topic '{$topic}' is unknown for cluster");
    }
}
