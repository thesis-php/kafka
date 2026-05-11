<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Scram;

/**
 * @internal
 */
enum HashAlgorithm: string
{
    case SHA256 = 'sha256';
    case SHA512 = 'sha512';
}
