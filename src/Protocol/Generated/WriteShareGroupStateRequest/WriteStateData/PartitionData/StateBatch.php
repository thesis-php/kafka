<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\WriteShareGroupStateRequest\WriteStateData\PartitionData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class StateBatch implements Encodable, Decodable
{
    /**
     * @param int $firstOffset The first offset of this state batch.
     * @param int $lastOffset The last offset of this state batch.
     * @param int $deliveryState The delivery state - 0:Available,2:Acked,4:Archived.
     * @param int $deliveryCount The delivery count.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $firstOffset,
        public int $lastOffset,
        public int $deliveryState,
        public int $deliveryCount,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt64($this->firstOffset);
        $writer->writeInt64($this->lastOffset);
        $writer->writeInt8($this->deliveryState);
        $writer->writeInt16($this->deliveryCount);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $firstOffset = $reader->readInt64();
        $lastOffset = $reader->readInt64();
        $deliveryState = $reader->readInt8();
        $deliveryCount = $reader->readInt16();
        $unknownTags = $reader->readTaggedFields();
        return new self($firstOffset, $lastOffset, $deliveryState, $deliveryCount, $unknownTags);
    }
}
