<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\OpenBatch;

/**
 * @internal
 */
enum State
{
    case Accumulate;
    case Released;
}
