<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class UnknownBroker extends Exception
{
    public function __construct(
        public readonly int $brokerId,
    ) {
        parent::__construct("Broker '{$brokerId}' is unknown for cluster");
    }
}
