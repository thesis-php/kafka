<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Amp\CancelledException;
use Thesis\Kafka\NetworkException;

/**
 * A request of the consumer group got no answer in time, also on the last
 * attempt. The time limit that fired is the previous exception.
 *
 * @api
 */
final class RequestTimedOut extends NetworkException
{
    public function __construct(CancelledException $previous)
    {
        parent::__construct('A request of the consumer group got no answer in time', previous: $previous);
    }
}
