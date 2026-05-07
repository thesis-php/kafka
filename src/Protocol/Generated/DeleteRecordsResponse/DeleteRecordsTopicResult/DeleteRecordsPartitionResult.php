<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteRecordsResponse\DeleteRecordsTopicResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteRecordsPartitionResult implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $lowWatermark The partition low water mark.
     * @param int $errorCode The deletion error code, or 0 if the deletion succeeded.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $lowWatermark,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->lowWatermark);
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $partitionIndex = $reader->readInt32();
        $lowWatermark = $reader->readInt64();
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $lowWatermark, $errorCode, $unknownTags);
    }
}
