<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

/**
 * @internal
 */
enum State
{
    case Idle;
    case Running;
    case Closed;
}
