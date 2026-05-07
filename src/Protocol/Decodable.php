<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 */
interface Decodable
{
    /**
     * @param non-negative-int $version
     */
    public static function decode(Reader $reader, int $version): static;
}
