<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Thesis\Kafka\Exception\CompressionFailed;

/**
 * @api
 */
interface Compressor
{
    public function codec(): Compressor\Codec;

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
