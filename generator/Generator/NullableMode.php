<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node\Stmt;

/**
 * @internal
 */
interface NullableMode
{
    /**
     * @template TAccessor
     * @param callable(TAccessor): list<Stmt> $nonNullable
     * @param callable(TAccessor): list<Stmt> $nullable
     * @param TAccessor $accessor
     * @return list<Stmt>
     */
    public function apply(
        callable $nonNullable,
        callable $nullable,
        mixed $accessor,
    ): array;
}
