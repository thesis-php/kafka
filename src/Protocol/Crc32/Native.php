<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Crc32;

use Thesis\Kafka\Protocol\Crc32;

/**
 * @api
 */
final readonly class Native implements Crc32
{
    #[\Override]
    public function compute(string $data): int
    {
        /** @var int $unsigned */
        $unsigned = unpack('N', hash('crc32c', $data, true))[1] ?? throw new \UnexpectedValueException(
            'Calculate of crc32c has failed for unexpected reason',
        );

        return $unsigned > 0x7FFFFFFF ? $unsigned - 0x100000000 : $unsigned;
    }
}
