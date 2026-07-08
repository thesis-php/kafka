<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

/**
 * @internal
 */
enum ConsumeState
{
    case Idle;
    case Running;
}
