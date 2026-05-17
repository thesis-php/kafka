<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

/**
 * @internal
 */
enum Reply
{
    case Expected;
    case None;
}
