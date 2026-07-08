<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\AssignmentConsumer;

/**
 * @internal
 */
enum ConsumerState
{
    case Idle;
    case Running;
}
