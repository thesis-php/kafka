<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;
use Thesis\Kafka\Protocol\Errors;

/**
 * @api
 */
final class ProtocolError extends Exception
{
    public function __construct(
        public readonly Errors $errors,
    ) {
        parent::__construct((string) $errors);
    }
}
