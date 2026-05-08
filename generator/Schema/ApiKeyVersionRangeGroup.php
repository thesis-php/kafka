<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final class ApiKeyVersionRangeGroup
{
    /** @var list<ApiKey> */
    public private(set) array $keys = [];

    public function __construct(
        public readonly int $min,
        public readonly int $max,
    ) {}

    public function addKey(ApiKey $key): void
    {
        $this->keys[] = $key;
    }
}
