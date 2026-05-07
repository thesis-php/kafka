<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\InitializeShareGroupStateRequest\InitializeStateData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionData implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $stateEpoch The state epoch for this share-partition.
     * @param int $startOffset The share-partition start offset, or -1 if the start offset is not being initialized.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $stateEpoch,
        public int $startOffset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt32($this->stateEpoch);
        $writer->writeInt64($this->startOffset);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $stateEpoch = $reader->readInt32();
        $startOffset = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $stateEpoch, $startOffset, $unknownTags);
    }
}
