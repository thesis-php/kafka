<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\TxnOffsetCommitRequest\TxnOffsetCommitRequestTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TxnOffsetCommitRequestPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The index of the partition within the topic.
     * @param int $committedOffset The message offset to be committed.
     * @param int $committedLeaderEpoch The leader epoch of the last consumed record.
     * @param ?string $committedMetadata Any associated metadata the client wants to keep.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $committedOffset,
        public int $committedLeaderEpoch,
        public ?string $committedMetadata,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->committedOffset);
        if ($version >= 2) {
            $writer->writeInt32($this->committedLeaderEpoch);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->committedMetadata);
        } else {
            $writer->writeNullableString($this->committedMetadata);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $partitionIndex = $reader->readInt32();
        $committedOffset = $reader->readInt64();
        $committedLeaderEpoch = -1;
        if ($version >= 2) {
            $committedLeaderEpoch = $reader->readInt32();
        }
        if ($flexible) {
            $committedMetadata = $reader->readCompactNullableString();
        } else {
            $committedMetadata = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $committedOffset, $committedLeaderEpoch, $committedMetadata, $unknownTags);
    }
}
