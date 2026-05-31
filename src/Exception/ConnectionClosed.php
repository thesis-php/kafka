<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\NetworkException;

/**
 * @api
 */
final class ConnectionClosed extends NetworkException
{
    public function __construct(string $message)
    {
        parent::__construct($message);
    }
}
