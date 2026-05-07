<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeShareGroupOffsetsResponse\DescribeShareGroupOffsetsResponseGroup\DescribeShareGroupOffsetsResponseTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsResponsePartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $startOffset The share-partition start offset.
     * @param int $leaderEpoch The leader epoch of the partition.
     * @param int $lag The share-partition lag.
     * @param int $errorCode The partition-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The partition-level error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $startOffset,
        public int $leaderEpoch,
        public int $lag,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->startOffset);
        $writer->writeInt32($this->leaderEpoch);
        if ($version >= 1) {
            $writer->writeInt64($this->lag);
        }
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $startOffset = $reader->readInt64();
        $leaderEpoch = $reader->readInt32();
        $lag = -1;
        if ($version >= 1) {
            $lag = $reader->readInt64();
        }
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $startOffset, $leaderEpoch, $lag, $errorCode, $errorMessage, $unknownTags);
    }
}
