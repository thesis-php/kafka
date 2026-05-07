<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 * @template-implements \IteratorAggregate<array-key, ApiKey>
 */
final readonly class ApiKeys implements \IteratorAggregate
{
    /** @var list<ApiKey> */
    public array $keys;

    /**
     * @param list<ApiKey> $keys
     */
    public function __construct(array $keys = [])
    {
        usort($keys, static fn(ApiKey $a, ApiKey $b) => $a->value <=> $b->value);
        $this->keys = $keys;
    }

    public function append(ApiKey $key): self
    {
        return new self(
            keys: [
                ...$this->keys,
                $key,
            ],
        );
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->keys;
    }
}
