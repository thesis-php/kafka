<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
final class UnknownBroker extends KafkaException
{
    public function __construct(
        public readonly int $brokerId,
    ) {
        parent::__construct("Broker '{$brokerId}' is unknown for cluster");
    }
}
