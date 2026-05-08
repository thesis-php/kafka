<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator\Ast;

use PhpParser\Node;
use PhpParser\PrettyPrinter\Standard;

/**
 * @internal
 */
final class Printer extends Standard
{
    #[\Override]
    protected function pMatchArm(Node\MatchArm $node): string
    {
        if ($node->conds === null) {
            return 'default => ' . $this->p($node->body);
        }

        if (\count($node->conds) === 1) {
            return $this->pKey($node->conds[0]) . $this->p($node->body);
        }

        $indent = $this->newline . str_repeat(' ', $this->indentLevel);

        return implode(
            separator: ',' . $indent,
            array: array_map($this->p(...), \array_slice($node->conds, 0, -1)),
        ) . ',' . $indent
            . $this->pKey($node->conds[array_key_last($node->conds)]) // @phpstan-ignore offsetAccess.notFound
            . $this->p($node->body);
    }
}
