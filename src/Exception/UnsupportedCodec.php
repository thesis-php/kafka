<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Compressor\Codec;
use Thesis\Kafka\Exception;

/**
 * @api
 */
final class UnsupportedCodec extends Exception
{
    public function __construct(
        public readonly Codec $codec,
    ) {
        parent::__construct("Codec {$this->codec->name} was not registered in the codec's registry.");
    }
}
