<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
interface Hasher
{
    public function hash(string $key): int;
}
