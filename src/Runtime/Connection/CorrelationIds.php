<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

/**
 * @internal
 */
final class CorrelationIds
{
    private const int MAX_CORRELATION_ID = (1 << 31) - 1;

    /** @var non-negative-int */
    public private(set) int $id { // @phpstan-ignore property.uninitialized
        get {
            $this->id ??= 0;

            if ($this->id >= self::MAX_CORRELATION_ID) {
                $this->id = 0;
            }

            return ++$this->id;
        }
    }
}
