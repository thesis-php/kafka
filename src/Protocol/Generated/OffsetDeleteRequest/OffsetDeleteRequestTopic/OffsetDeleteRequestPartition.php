<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetDeleteRequest\OffsetDeleteRequestTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetDeleteRequestPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     */
    public function __construct(
        public int $partitionIndex,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        return new self($partitionIndex);
    }
}
