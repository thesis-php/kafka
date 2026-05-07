<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node\Expr;

/**
 * @internal
 */
final readonly class VersionGatedNullable implements NullableMode
{
    public function __construct(
        private Expr $condition,
    ) {}

    #[\Override]
    public function apply(
        callable $nonNullable,
        callable $nullable,
        mixed $accessor,
    ): array {
        return [
            Ast::ifElse(
                $this->condition,
                $nullable($accessor),
                $nonNullable($accessor),
            ),
        ];
    }
}
