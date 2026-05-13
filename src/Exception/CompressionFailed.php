<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
final class CompressionFailed extends KafkaException
{
    public function __construct(
        string $reason,
        ?\Throwable $previous = null,
    ) {
        parent::__construct(
            "Compression failed: {$reason}",
            previous: $previous,
        );
    }
}
