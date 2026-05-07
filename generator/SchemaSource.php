<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator;

/**
 * @internal
 */
interface SchemaSource
{
    /**
     * @return iterable<array-key, non-empty-string>
     */
    public function schemas(): iterable;
}
