<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final readonly class ApiKey
{
    /**
     * @param non-empty-string $name
     * @param non-negative-int $value
     * @param non-negative-int $min
     * @param non-negative-int $max
     * @param list<non-empty-string> $listeners
     */
    public function __construct(
        public string $name,
        public int $value,
        public int $min,
        public int $max,
        public VersionRange $flexibleVersions = new VersionRange(),
        public array $listeners = [],
    ) {}
}
