<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
interface Crc32
{
    /**
     * @param non-empty-string $data
     */
    public function compute(string $data): int;
}
