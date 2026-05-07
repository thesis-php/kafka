<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * Stores raw flexible-version tagged fields that are unknown to the generated schema.
 *
 * @api
 * @template-implements \ArrayAccess<int, string>
 * @template-implements \IteratorAggregate<int, string>
 */
final class UnknownTags implements
    \ArrayAccess,
    \Countable,
    \IteratorAggregate
{
    /**
     * @param array<int, string> $keyvals
     */
    public function __construct(private array $keyvals = []) {}

    /**
     * @param callable(int, string): void $f
     */
    public function each(callable $f): void
    {
        $ordered = array_keys($this->keyvals);
        sort($ordered, SORT_NUMERIC);

        foreach ($ordered as $key) {
            $f($key, $this->keyvals[$key]); // @phpstan-ignore offsetAccess.notFound
        }
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->keyvals);
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->keyvals;
    }

    #[\Override]
    public function offsetExists(mixed $offset): bool
    {
        return \array_key_exists($offset, $this->keyvals);
    }

    #[\Override]
    public function offsetGet(mixed $offset): ?string
    {
        return $this->keyvals[$offset] ?? null;
    }

    #[\Override]
    public function offsetSet(mixed $offset, mixed $value): void
    {
        $this->keyvals[(int) $offset] = $value;
    }

    #[\Override]
    public function offsetUnset(mixed $offset): void
    {
        unset($this->keyvals[$offset]);
    }
}
