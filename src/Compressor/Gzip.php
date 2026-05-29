<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Exception\MissingExtension;

/**
 * @api
 */
final readonly class Gzip implements Compressor
{
    /**
     * @throws MissingExtension
     */
    public function __construct(
        private int $level = 6,
    ) {
        requiresExtension('zlib');
    }

    public static function supported(): bool
    {
        return \extension_loaded('zlib');
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
