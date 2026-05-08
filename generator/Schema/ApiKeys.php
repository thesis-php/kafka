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

    /**
     * @return list<ApiKeyListenerGroup>
     */
    public function groupListeners(): array
    {
        return $this->groupBy(
            static fn(ApiKey $key) => implode("\0", $key->listeners),
            static fn(ApiKey $key) => new ApiKeyListenerGroup($key->listeners),
        );
    }

    /**
     * @return list<ApiKeyVersionRangeGroup>
     */
    public function groupVersionRanges(): array
    {
        return $this->groupBy(
            static fn(ApiKey $key) => "{$key->min}:{$key->max}",
            static fn(ApiKey $key) => new ApiKeyVersionRangeGroup($key->min, $key->max),
        );
    }

    /**
     * @return list<ApiKeyFlexibleVersionGroup>
     */
    public function groupFlexibleVersions(): array
    {
        return $this->groupBy(
            static fn(ApiKey $key) => "{$key->flexibleVersions->min}:{$key->flexibleVersions->max}",
            static fn(ApiKey $key) => new ApiKeyFlexibleVersionGroup($key->flexibleVersions),
        );
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->keys;
    }

    /**
     * @template T of ApiKeyListenerGroup|ApiKeyFlexibleVersionGroup|ApiKeyVersionRangeGroup
     * @param callable(ApiKey): string $keyBy
     * @param callable(ApiKey): T $factory
     * @return list<T>
     */
    private function groupBy(callable $keyBy, callable $factory): array
    {
        $groups = [];

        foreach ($this->keys as $key) {
            $group = $keyBy($key);

            $groups[$group] ??= $factory($key);
            $groups[$group]->addKey($key);
        }

        return array_values($groups);
    }
}
