<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Compressor;

use Thesis\Kafka\Exception\MissingExtension;
use Thesis\Kafka\Protocol\Compressor;

/**
 * @api
 */
final readonly class Lz4 implements Compressor
{
    /**
     * @throws MissingExtension
     */
    public function __construct()
    {
        if (!\extension_loaded('lz4')) {
            throw new MissingExtension('lz4');
        }
    }

    #[\Override]
    public function codec(): Codec
    {
        return Codec::Lz4;
    }

    #[\Override]
    public function compress(string $data): string
    {
        return namespace\compress(lz4_compress(...), $data); // @phpstan-ignore function.notFound
    }

    #[\Override]
    public function decompress(string $data): string
    {
        return namespace\decompress(lz4_uncompress(...), $data); // @phpstan-ignore function.notFound
    }
}
