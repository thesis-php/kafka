<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use Thesis\Kafka\Exception\IncompatibleVersions;

/**
 * @internal
 */
final readonly class VersionRange
{
    /**
     * @param non-negative-int $min
     * @param non-negative-int $max
     */
    public function __construct(
        public int $min,
        public int $max,
    ) {
        \assert($max >= $min, 'max version must be greater than min version');
    }

    /**
     * @throws IncompatibleVersions
     */
    public function negotiate(self $broker): int
    {
        $min = max($this->min, $broker->min);
        $max = min($this->max, $broker->max);

        if ($max < $min) {
            throw new IncompatibleVersions($this, $broker);
        }

        return $max;
    }
}
