<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final readonly class Message
{
    /**
     * @param non-empty-string $name
     * @param ?non-negative-int $apiKey
     * @param list<Field> $fields
     * @param list<CommonStruct> $commonStructs
     */
    public function __construct(
        public Type $type,
        public string $name,
        public VersionRange $validVersions = new VersionRange(),
        public VersionRange $flexibleVersions = new VersionRange(),
        public ?int $apiKey = null,
        public array $fields = [],
        public array $commonStructs = [],
    ) {}
}
