<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node\Expr;

/**
 * @internal
 */
final readonly class EncodeContext
{
    public function __construct(
        public Expr $writer,
        public FlexibleCondition $flexible,
        public ?string $fieldName = null,
    ) {}

    public function forElement(): self
    {
        return new self($this->writer, $this->flexible);
    }
}
