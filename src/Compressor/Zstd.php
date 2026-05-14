<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Exception\MissingExtension;

/**
 * @api
 */
final readonly class Zstd implements Compressor
{
    /**
     * @throws MissingExtension
     */
    public function __construct()
    {
        if (!\extension_loaded('zstd')) {
            throw new MissingExtension('zstd');
        }
    }

    #[\Override]
    public function codec(): Codec
    {
        return Codec::Zstd;
    }

    #[\Override]
    public function compress(string $data): string
    {
        return namespace\compress(zstd_compress(...), $data);
    }

    #[\Override]
    public function decompress(string $data): string
    {
        return namespace\decompress(zstd_uncompress(...), $data);
    }
}
