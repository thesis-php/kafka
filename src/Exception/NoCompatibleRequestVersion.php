<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final class NoCompatibleRequestVersion extends KafkaException
{
    public function __construct(
        public readonly Generated\ApiKey $key,
    ) {
        $range = $key->clientVersionRange();

        parent::__construct("No compatible broker version for api key '{$key->value}' found, client supports only: [{$range->min}:{$range->max}].");
    }
}
