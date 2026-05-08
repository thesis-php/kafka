<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

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

    public static function fromApiVersion(Generated\ApiVersionsResponse\ApiVersion $version): self
    {
        /** @var non-negative-int $min */
        $min = $version->minVersion;

        /** @var non-negative-int $max */
        $max = $version->maxVersion;

        return new self($min, $max);
    }

    /**
     * @return ?non-negative-int
     */
    public function negotiate(self $broker): ?int
    {
        $min = max($this->min, $broker->min);
        $max = min($this->max, $broker->max);

        if ($max < $min) {
            return null;
        }

        return $max;
    }
}
