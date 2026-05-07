<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetFetchResponse\OffsetFetchResponseTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchResponsePartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $committedOffset The committed message offset.
     * @param int $committedLeaderEpoch The leader epoch.
     * @param ?string $metadata The partition metadata.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $committedOffset,
        public int $committedLeaderEpoch,
        public ?string $metadata,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->committedOffset);
        if ($version >= 5) {
            $writer->writeInt32($this->committedLeaderEpoch);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->metadata);
        } else {
            $writer->writeNullableString($this->metadata);
        }
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $partitionIndex = $reader->readInt32();
        $committedOffset = $reader->readInt64();
        $committedLeaderEpoch = -1;
        if ($version >= 5) {
            $committedLeaderEpoch = $reader->readInt32();
        }
        if ($flexible) {
            $metadata = $reader->readCompactNullableString();
        } else {
            $metadata = $reader->readNullableString();
        }
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $committedOffset, $committedLeaderEpoch, $metadata, $errorCode, $unknownTags);
    }
}
