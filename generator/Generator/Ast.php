<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use PhpParser\Node;
use PhpParser\Node\Expr;
use PhpParser\Node\Scalar;
use PhpParser\Node\Stmt;
use PhpParser\PrettyPrinter;

/**
 * @internal
 */
final readonly class Ast
{
    public static function var(string $name): Expr\Variable
    {
        return new Expr\Variable($name);
    }

    public static function this(): Expr\Variable
    {
        return new Expr\Variable('this');
    }

    public static function int(int $value): Scalar\Int_
    {
        return new Scalar\Int_($value);
    }

    public static function bool(bool $value): Expr\ConstFetch
    {
        return new Expr\ConstFetch(new Node\Name($value ? 'true' : 'false'));
    }

    public static function float(float $value): Scalar\Float_
    {
        return new Scalar\Float_($value);
    }

    public static function str(string $value): Scalar\String_
    {
        return new Scalar\String_($value);
    }

    /**
     * @no-named-arguments
     */
    public static function interpolated(string|Expr ...$parts): Scalar\InterpolatedString
    {
        return new Scalar\InterpolatedString(
            array_map(
                static fn(string|Expr $part): Node|Expr => \is_string($part)
                    ? new Node\InterpolatedStringPart($part)
                    : $part,
                $parts,
            ),
        );
    }

    public static function null(): Expr\ConstFetch
    {
        return new Expr\ConstFetch(new Node\Name('null'));
    }

    /**
     * @no-named-arguments
     */
    public static function array(Expr ...$items): Expr\Array_
    {
        return new Expr\Array_(
            array_map(static fn(Expr $item): Node\ArrayItem => new Node\ArrayItem($item), $items),
            ['kind' => Expr\Array_::KIND_SHORT],
        );
    }

    public static function arrayGet(Expr $array, Expr $key): Expr\ArrayDimFetch
    {
        return new Expr\ArrayDimFetch($array, $key);
    }

    /**
     * @param non-empty-string $class
     * @param non-empty-string $name
     */
    public static function classConst(string $class, string $name): Expr\ClassConstFetch
    {
        return new Expr\ClassConstFetch(new Node\Name($class), $name);
    }

    /**
     * @param non-empty-string $name
     */
    public static function prop(Expr $object, string $name): Expr\PropertyFetch
    {
        return new Expr\PropertyFetch($object, $name);
    }

    /**
     * @no-named-arguments
     * @param non-empty-string $class
     */
    public static function new(string $class, Expr ...$args): Expr\New_
    {
        return new Expr\New_(
            str_starts_with($class, '\\')
                ? new Node\Name\FullyQualified(substr($class, 1))
                : new Node\Name($class),
            self::args($args),
        );
    }

    public static function throw(Expr $expr): Expr\Throw_
    {
        return new Expr\Throw_($expr);
    }

    public static function castString(Expr $expr): Expr\Cast\String_
    {
        return new Expr\Cast\String_($expr);
    }

    /**
     * @no-named-arguments
     * @param non-empty-string $method
     */
    public static function call(Expr $object, string $method, Expr ...$args): Expr\MethodCall
    {
        return new Expr\MethodCall($object, $method, self::args($args));
    }

    /**
     * @no-named-arguments
     * @param non-empty-string $method
     */
    public static function nullsafeCall(Expr $object, string $method, Expr ...$args): Expr\NullsafeMethodCall
    {
        return new Expr\NullsafeMethodCall($object, $method, self::args($args));
    }

    /**
     * @no-named-arguments
     * @param non-empty-string $class
     * @param non-empty-string $method
     */
    public static function staticCall(string $class, string $method, Expr ...$args): Expr\StaticCall
    {
        return new Expr\StaticCall(
            str_starts_with($class, '\\')
                ? new Node\Name\FullyQualified(substr($class, 1))
                : new Node\Name($class),
            $method,
            self::args($args),
        );
    }

    /**
     * @no-named-arguments
     * @param non-empty-string $name
     */
    public static function funcCall(string $name, Expr ...$args): Expr\FuncCall
    {
        return new Expr\FuncCall(
            str_starts_with($name, '\\')
                ? new Node\Name\FullyQualified(substr($name, 1))
                : new Node\Name($name),
            self::args($args),
        );
    }

    public static function ge(Expr $left, Expr $right): Expr\BinaryOp\GreaterOrEqual
    {
        return new Expr\BinaryOp\GreaterOrEqual($left, $right);
    }

    public static function le(Expr $left, Expr $right): Expr\BinaryOp\SmallerOrEqual
    {
        return new Expr\BinaryOp\SmallerOrEqual($left, $right);
    }

    public static function lt(Expr $left, Expr $right): Expr\BinaryOp\Smaller
    {
        return new Expr\BinaryOp\Smaller($left, $right);
    }

    public static function eq(Expr $left, Expr $right): Expr\BinaryOp\Identical
    {
        return new Expr\BinaryOp\Identical($left, $right);
    }

    public static function neq(Expr $left, Expr $right): Expr\BinaryOp\NotIdentical
    {
        return new Expr\BinaryOp\NotIdentical($left, $right);
    }

    /**
     * @no-named-arguments
     */
    public static function isset(Expr ...$vars): Expr\Isset_
    {
        return new Expr\Isset_($vars);
    }

    /**
     * @no-named-arguments
     */
    public static function and(Expr ...$exprs): Expr
    {
        \assert($exprs !== []);

        return array_reduce(
            \array_slice($exprs, 1),
            static fn(Expr $acc, Expr $e): Expr => new Expr\BinaryOp\BooleanAnd($acc, $e),
            $exprs[0],
        );
    }

    public static function ternary(Expr $cond, ?Expr $then, Expr $else): Expr\Ternary
    {
        return new Expr\Ternary($cond, $then, $else);
    }

    public static function preinc(Expr $var): Expr\PreInc
    {
        return new Expr\PreInc($var);
    }

    /**
     * @no-named-arguments
     */
    public static function match(Expr $subject, Node\MatchArm ...$arms): Expr\Match_
    {
        return new Expr\Match_($subject, $arms);
    }

    public static function arm(Expr $cond, Expr $body): Node\MatchArm
    {
        return new Node\MatchArm([$cond], $body);
    }

    public static function defaultArm(Expr $body): Node\MatchArm
    {
        return new Node\MatchArm(null, $body);
    }

    public static function return(?Expr $expr = null): Stmt\Return_
    {
        return new Stmt\Return_($expr);
    }

    public static function assign(Expr $target, Expr $value): Stmt\Expression
    {
        return new Stmt\Expression(new Expr\Assign($target, $value));
    }

    public static function push(Expr $array, Expr $value): Stmt\Expression
    {
        return new Stmt\Expression(
            new Expr\Assign(new Expr\ArrayDimFetch($array, null), $value),
        );
    }

    public static function stmt(Expr $expr): Stmt\Expression
    {
        return new Stmt\Expression($expr);
    }

    /**
     * @no-named-arguments
     */
    public static function if(Expr $cond, Stmt ...$body): Stmt\If_
    {
        return new Stmt\If_($cond, ['stmts' => $body]);
    }

    /**
     * @no-named-arguments
     */
    public static function unset(Expr ...$vars): Stmt\Unset_
    {
        return new Stmt\Unset_($vars);
    }

    /**
     * @param list<Stmt> $then
     * @param list<Stmt> $else
     */
    public static function ifElse(Expr $cond, array $then, array $else): Stmt\If_
    {
        return new Stmt\If_($cond, [
            'stmts' => $then,
            'else' => new Stmt\Else_($else),
        ]);
    }

    /**
     * @param list<Stmt> $body
     */
    public static function foreach(Expr $array, Expr $value, array $body): Stmt\Foreach_
    {
        return new Stmt\Foreach_($array, $value, ['stmts' => $body]);
    }

    /**
     * @param list<Stmt> $body
     */
    public static function for(Stmt\Expression $init, Expr $cond, Expr $loop, array $body): Stmt\For_
    {
        return new Stmt\For_([
            'init' => [$init->expr],
            'cond' => [$cond],
            'loop' => [$loop],
            'stmts' => $body,
        ]);
    }

    /**
     * @no-named-arguments
     */
    public static function body(Stmt ...$stmts): string
    {
        return self::line(...$stmts) . PHP_EOL;
    }

    /**
     * @no-named-arguments
     */
    public static function line(Stmt ...$stmts): string
    {
        return new PrettyPrinter\Standard()->prettyPrint($stmts);
    }

    /**
     * @param list<Expr> $exprs
     * @return list<Node\Arg>
     */
    private static function args(array $exprs): array
    {
        return array_map(static fn(Expr $e): Node\Arg => new Node\Arg($e), $exprs);
    }
}
