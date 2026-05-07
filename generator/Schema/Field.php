<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final readonly class Field
{
    /**
     * @param non-empty-string $name
     * @param non-empty-string $type
     * @param ?non-negative-int $tag
     * @param ?non-empty-string $about
     * @param list<Field> $fields
     */
    public function __construct(
        public string $name,
        public string $type,
        public VersionRange $versions,
        public ?VersionRange $flexibleVersions = null,
        public VersionRange $nullableVersions = new VersionRange(),
        public VersionRange $taggedVersions = new VersionRange(),
        public ?int $tag = null,
        public ?string $about = null,
        public mixed $default = null,
        public array $fields = [],
    ) {}
}
