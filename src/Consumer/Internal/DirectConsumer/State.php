<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\DirectConsumer;

/**
 * @internal
 */
enum State
{
    case Idle;
    case Starting;
    case Running;
    case Closed;
}
