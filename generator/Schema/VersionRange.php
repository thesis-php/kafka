<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

use CuyZ\Valinor\Mapper\Object\Constructor;

/**
 * @internal
 */
final readonly class VersionRange
{
    /**
     * @param ?non-negative-int $min null only when the range is "none"
     * @param ?non-negative-int $max null when unbounded ("X+")
     */
    public function __construct(
        public ?int $min = null,
        public ?int $max = null,
    ) {}

    public static function none(): self
    {
        return new self(null, null);
    }

    /**
     * @param ?non-empty-string $spec
     */
    #[Constructor]
    public static function fromString(?string $spec = null): self
    {
        $spec ??= 'none';
        if ($spec === 'none') {
            return self::none();
        }

        if (str_ends_with($spec, '+')) {
            $min = (int) substr($spec, 0, -1);
            \assert($min >= 0);

            return new self($min, null);
        }

        if (str_contains($spec, '-')) {
            $range = explode('-', $spec, 2);
            \assert(\count($range) === 2);

            [$min, $max] = $range;
            $minInt = (int) $min;
            $maxInt = (int) $max;
            \assert($minInt >= 0 && $maxInt >= $minInt);

            return new self($minInt, $maxInt);
        }

        $v = (int) $spec;
        \assert($v >= 0);

        return new self($v, $v);
    }

    public function isNone(): bool
    {
        return $this->min === null;
    }

    public function covers(self $within): bool
    {
        if ($this->isNone()) {
            return $within->isNone();
        }

        if ($within->isNone()) {
            return true;
        }

        if ($this->min > $within->min) {
            return false;
        }

        if ($this->max === null) {
            return true;
        }

        return $within->max !== null && $this->max >= $within->max;
    }

    /**
     * The part of this range that is not in $other. Null if that part is
     * empty or is not one continuous range.
     */
    public function without(self $other): ?self
    {
        if ($this->min === null || $other->min === null || $this->disjoint($other)) {
            return null;
        }

        $startsInside = $other->min <= $this->min;
        $endsInside = $other->max === null || ($this->max !== null && $other->max >= $this->max);

        if ($startsInside && !$endsInside) {
            return new self($other->max + 1, $this->max);
        }

        if ($endsInside && !$startsInside) {
            return new self($this->min, $other->min - 1);
        }

        return null;
    }

    /**
     * The part of this range that is also in $other.
     */
    public function intersect(self $other): self
    {
        if ($this->min === null || $other->min === null || $this->disjoint($other)) {
            return self::none();
        }

        $max = match (true) {
            $this->max === null => $other->max,
            $other->max === null => $this->max,
            default => min($this->max, $other->max),
        };

        return new self(max($this->min, $other->min), $max);
    }

    public function disjoint(self $other): bool
    {
        if ($this->isNone() || $other->isNone()) {
            return true;
        }

        if ($this->max !== null && $this->max < $other->min) {
            return true;
        }

        return $other->max !== null && $other->max < $this->min;
    }
}
