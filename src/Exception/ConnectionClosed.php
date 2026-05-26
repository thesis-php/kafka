<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class ConnectionClosed extends Exception
{
    public function __construct(string $message)
    {
        parent::__construct($message);
    }
}
