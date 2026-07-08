<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\AssignmentConsumer;

/**
 * @internal
 */
enum ConsumeState
{
    case Idle;
    case Running;
}
