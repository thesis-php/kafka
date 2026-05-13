<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Compressor;

/**
 * @api
 */
enum Codec: int
{
    case None = 0;
    case Gzip = 1;
    case Snappy = 2;
    case Lz4 = 3;
    case Zstd = 4;
}
