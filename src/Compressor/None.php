<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Compressor;

/**
 * @api
 */
enum None implements Compressor
{
    case Compressor;

    #[\Override]
    public function codec(): Compressor\Codec
    {
        return Codec::None;
    }

    #[\Override]
    public function compress(string $data): string
    {
        return $data;
    }

    #[\Override]
    public function decompress(string $data): string
    {
        return $data;
    }
}
