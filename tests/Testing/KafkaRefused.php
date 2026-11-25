<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Thesis\Kafka\Exception;

/**
 * The broker returned an error for a request of the kit.
 *
 * @internal
 */
final class KafkaRefused extends Exception {}
