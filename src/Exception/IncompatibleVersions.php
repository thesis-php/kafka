<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\VersionRange;

/**
 * @api
 */
final class IncompatibleVersions extends KafkaException
{
    public function __construct(
        public readonly VersionRange $client,
        public readonly VersionRange $broker,
    ) {
        parent::__construct("Version ranges do not overlap: client supports [{$client->min}, {$client->max}], broker supports [{$broker->min}, {$broker->max}]");
    }
}
