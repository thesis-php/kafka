<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Producer\OpenBatch;

/**
 * @internal
 */
enum State
{
    case Accumulate;
    case Released;
}
