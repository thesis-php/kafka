<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Compressor;

use Thesis\Kafka\Exception\MissingExtension;
use Thesis\Kafka\Protocol;

/**
 * @api
 */
final readonly class Gzip implements Protocol\Compressor
{
    /**
     * @throws MissingExtension
     */
    public function __construct(
        private int $level = 6,
    ) {
        if (!\extension_loaded('zlib')) {
            throw new MissingExtension('zlib');
        }
    }

    #[\Override]
    public function codec(): Codec
    {
        return Codec::Gzip;
    }

    #[\Override]
    public function compress(string $data): string
    {
        return namespace\compress(fn(string $data) => gzencode($data, $this->level), $data);
    }

    #[\Override]
    public function decompress(string $data): string
    {
        return namespace\decompress(gzdecode(...), $data);
    }
}
