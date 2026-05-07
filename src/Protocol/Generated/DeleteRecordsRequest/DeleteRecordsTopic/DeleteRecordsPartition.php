<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteRecordsRequest\DeleteRecordsTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteRecordsPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $offset The deletion offset. -1 means that records should be truncated to the high watermark.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $offset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->offset);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $partitionIndex = $reader->readInt32();
        $offset = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $offset, $unknownTags);
    }
}
