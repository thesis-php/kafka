<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
final class ConnectionClosed extends KafkaException
{
    public function __construct(string $message)
    {
        parent::__construct($message);
    }
}
