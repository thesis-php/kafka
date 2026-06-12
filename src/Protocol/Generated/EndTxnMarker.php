<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class EndTxnMarker implements Encodable, Decodable
{
    /**
     * @param int $coordinatorEpoch The coordinator epoch when appending the record
     */
    public function __construct(
        public int $coordinatorEpoch,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->coordinatorEpoch);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $coordinatorEpoch = $reader->readInt32();
        return new self($coordinatorEpoch);
    }
}
