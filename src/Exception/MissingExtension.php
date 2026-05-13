<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
final class MissingExtension extends KafkaException
{
    public function __construct(
        public readonly string $extension,
    ) {
        parent::__construct(
            "Required PHP extension is not loaded: {$extension}",
        );
    }
}
