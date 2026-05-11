<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
abstract class MalformedResponse extends KafkaException {}
