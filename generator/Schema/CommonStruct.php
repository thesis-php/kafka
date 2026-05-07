<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final readonly class CommonStruct
{
    /**
     * @param non-empty-string $name
     * @param list<Field> $fields
     */
    public function __construct(
        public string $name,
        public VersionRange $versions,
        public array $fields = [],
    ) {}
}
