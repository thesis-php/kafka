<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception;

/**
 * @api
 */
final class ConnectionFailed extends Exception
{
    public function __construct(
        public readonly Endpoint $endpoint,
        \Throwable $cause,
    ) {
        parent::__construct(
            "Unable to connect to {$endpoint}: {$cause->getMessage()}",
            previous: $cause,
        );
    }
}
