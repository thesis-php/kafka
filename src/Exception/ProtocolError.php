<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Errors;

/**
 * @api
 */
final class ProtocolError extends KafkaException
{
    public function __construct(
        public readonly Errors $errors,
    ) {
        parent::__construct(\sprintf('Kafka request failed with one or more protocol errors, use %s::errors for details.', self::class));
    }
}
