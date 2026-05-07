<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\SchemaSource;

use Thesis\Kafka\Generator\SchemaSource;

/**
 * @internal
 */
final readonly class StaticSource implements SchemaSource
{
    /**
     * @param non-empty-list<non-empty-string> $schemas
     */
    public function __construct(
        private array $schemas,
    ) {}

    #[\Override]
    public function schemas(): iterable
    {
        return $this->schemas;
    }
}
