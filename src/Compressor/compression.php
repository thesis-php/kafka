<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Exception\CompressionFailed;
use Thesis\Kafka\Exception\MissingExtension;
use function Thesis\exceptionally;

/**
 * @api
 * @param non-empty-string $extension
 * @throws MissingExtension
 */
function requiresExtension(string $extension): void
{
    if (!\extension_loaded($extension)) {
        throw new MissingExtension($extension);
    }
}

/**
 * @api
 * @param callable(non-empty-string): (false|string) $compressor
 * @param non-empty-string $data
 * @return non-empty-string
 * @throws CompressionFailed
 */
function compress(callable $compressor, string $data): string
{
    try {
        $compressed = exceptionally(static fn() => $compressor($data));
    } catch (\ErrorException $e) {
        throw new CompressionFailed($e->getMessage(), $e);
    }

    if ($compressed === '') {
        throw new CompressionFailed('Compressed output is empty.');
    }

    return $compressed;
}

/**
 * @api
 * @param callable(non-empty-string): (false|string) $decompressor
 * @param non-empty-string $data
 * @return non-empty-string
 * @throws CompressionFailed
 */
function decompress(callable $decompressor, string $data): string
{
    try {
        $decompressed = exceptionally(static fn() => $decompressor($data));
    } catch (\ErrorException $e) {
        throw new CompressionFailed($e->getMessage(), $e);
    }

    if ($decompressed === '') {
        throw new CompressionFailed('Decompressed output is empty.');
    }

    return $decompressed;
}
