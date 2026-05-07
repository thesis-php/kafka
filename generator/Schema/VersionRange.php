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
