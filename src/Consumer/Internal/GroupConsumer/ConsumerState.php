<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

/**
 * @internal
 */
enum ConsumerState
{
    case Idle;
    case Running;
    case Closed;
}
