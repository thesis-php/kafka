<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Record;

/**
 * @internal
 */
enum TimestampType: int
{
    case CreateTime = 0;
    case LogAppendTime = 1;
}
