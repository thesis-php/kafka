<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetDeleteResponse\OffsetDeleteResponseTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetDeleteResponsePartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $errorCode The error code, or 0 if there was no error.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        return new self($partitionIndex, $errorCode);
    }
}
