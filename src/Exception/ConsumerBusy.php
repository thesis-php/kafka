<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class ConsumerBusy extends Exception
{
    public function __construct()
    {
        parent::__construct(
            'consume() is already running on this consumer; only one consume() loop may be active at a time.',
        );
    }
}
