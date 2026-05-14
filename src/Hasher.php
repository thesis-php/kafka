<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
interface Hasher
{
    /**
     * @param non-empty-string $key
     */
    public function hash(string $key): int;
}
