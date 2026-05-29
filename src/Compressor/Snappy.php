<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Exception\MissingExtension;

/**
 * @api
 */
final readonly class Snappy implements Compressor
{
    /**
     * @throws MissingExtension
     */
    public function __construct()
    {
        requiresExtension('snappy');
    }

    public static function supported(): bool
    {
        return \extension_loaded('snappy');
    }

    #[\Override]
    public function codec(): Codec
    {
        return Codec::Snappy;
    }

    #[\Override]
    public function compress(string $data): string
    {
        return namespace\compress(snappy_compress(...), $data);
    }

    #[\Override]
    public function decompress(string $data): string
    {
        return namespace\decompress(snappy_uncompress(...), $data);
    }
}
