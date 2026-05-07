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
    ) {}
}
