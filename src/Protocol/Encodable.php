<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 */
interface Encodable
{
    /**
     * @param non-negative-int $version
     */
    public function encode(Writer $writer, int $version): void;
}
