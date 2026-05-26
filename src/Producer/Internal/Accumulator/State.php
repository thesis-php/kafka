<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Accumulator;

/**
 * @internal
 */
enum State
{
    case Ready;
    case Dead;
}
