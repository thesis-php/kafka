<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

/**
 * Where a consumer starts when it has no committed offset, and where it goes
 * on when its position is out of the range of the log.
 *
 * @api
 */
enum AutoOffsetReset: int
{
    /**
     * Do not choose a position. A group consumer without a committed offset
     * throws {@see \Thesis\Kafka\Exception\NoOffsetForPartitions}. A position
     * out of range throws {@see \Thesis\Kafka\Exception\OffsetOutOfRange}.
     * A consumer without a group cannot use it: {@see \Thesis\Kafka\Client::createConsumer()}
     * throws {@see \Thesis\Kafka\Exception\InvalidConfiguration}.
     */
    case None = 0;

    /** Start from the earliest available offset in the log. */
    case Earliest = -2;

    /** Start from the next offset to be produced (only new records). */
    case Latest = -1;
}
