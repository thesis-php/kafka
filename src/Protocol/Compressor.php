<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use Thesis\Kafka\Exception\CompressionFailed;
use Thesis\Kafka\Protocol\Compressor\Codec;

/**
 * @api
 */
interface Compressor
{
    public function codec(): Codec;

    /**
     * @param non-empty-string $data
     * @return non-empty-string
     * @throws CompressionFailed
     */
    public function compress(string $data): string;

    /**
     * @param non-empty-string $data
     * @return non-empty-string
     * @throws CompressionFailed
     */
    public function decompress(string $data): string;
}
