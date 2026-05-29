<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

/**
 * @api
 */
enum AutoOffsetReset: int
{
    /** Start from the earliest available offset in the log. */
    case Earliest = -2;

    /** Start from the next offset to be produced (only new records). */
    case Latest = -1;
}
