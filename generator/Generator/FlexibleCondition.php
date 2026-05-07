<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node\Expr;
use PhpParser\Node\Stmt;

/**
 * @internal
 */
final readonly class FlexibleCondition
{
    private function __construct(
        public FlexibleKind $kind,
        public Expr $expr,
    ) {}

    public static function always(): self
    {
        return new self(FlexibleKind::Always, Ast::bool(true));
    }

    public static function never(): self
    {
        return new self(FlexibleKind::Never, Ast::bool(false));
    }

    public static function gated(Expr $condition): self
    {
        return new self(FlexibleKind::Gated, $condition);
    }

    public function branch(Stmt $whenFlexible, Stmt $whenNotFlexible): Stmt
    {
        return match ($this->kind) {
            FlexibleKind::Always => $whenFlexible,
            FlexibleKind::Never => $whenNotFlexible,
            FlexibleKind::Gated => Ast::ifElse($this->expr, [$whenFlexible], [$whenNotFlexible]),
        };
    }

    /**
     * @param list<Stmt> $stmts
     * @return list<Stmt>
     */
    public function gate(array $stmts): array
    {
        return match ($this->kind) {
            FlexibleKind::Always => $stmts,
            FlexibleKind::Never => [],
            FlexibleKind::Gated => [Ast::if($this->expr, ...$stmts)],
        };
    }

    /**
     * @param list<Stmt> $whenFlexible
     * @param list<Stmt> $whenNotFlexible
     * @return list<Stmt>
     */
    public function branchStmts(array $whenFlexible, array $whenNotFlexible): array
    {
        return match ($this->kind) {
            FlexibleKind::Always => $whenFlexible,
            FlexibleKind::Never => $whenNotFlexible,
            FlexibleKind::Gated => [Ast::ifElse($this->expr, $whenFlexible, $whenNotFlexible)],
        };
    }
}
