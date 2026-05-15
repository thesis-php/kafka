<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

/**
 * @internal
 */
enum State
{
    case Idle;
    case Running;
    case Closed;
}
