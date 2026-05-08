<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final class ApiKeyFlexibleVersionGroup
{
    /** @var list<ApiKey> */
    public private(set) array $keys = [];

    public function __construct(
        public readonly VersionRange $flexibleVersions,
    ) {}

    public function addKey(ApiKey $key): void
    {
        $this->keys[] = $key;
    }
}
