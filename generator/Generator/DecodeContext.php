<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node\Expr;

/**
 * @internal
 */
final readonly class DecodeContext
{
    public function __construct(
        public Expr $reader,
        public FlexibleCondition $flexible,
        public ?FlexibleCondition $nullableFlexible = null,
        public ?FlexibleCondition $nonNullableFlexible = null,
    ) {}

    public function forElement(): self
    {
        return new self($this->reader, $this->flexible);
    }

    public function whenNullable(): FlexibleCondition
    {
        return $this->nullableFlexible ?? $this->flexible;
    }

    public function whenNonNullable(): FlexibleCondition
    {
        return $this->nonNullableFlexible ?? $this->flexible;
    }
}
