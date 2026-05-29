<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\AsyncConsumer;

/**
 * @internal
 */
enum ConsumeState
{
    case Ready;
    case Running;
}
