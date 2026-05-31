<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Sender\PartitionWorker;

/**
 * @internal
 */
enum State
{
    case Running;
    case Closed;
}
