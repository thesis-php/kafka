<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Producer\Accumulator;

/**
 * @internal
 */
enum State
{
    case Ready;
    case Dead;
}
