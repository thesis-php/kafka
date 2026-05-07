<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetForLeaderEpochResponse\OffsetForLeaderTopicResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class EpochEndOffset implements Encodable, Decodable
{
    /**
     * @param int $errorCode The error code 0, or if there was no error.
     * @param int $partition The partition index.
     * @param int $leaderEpoch The leader epoch of the partition.
     * @param int $endOffset The end offset of the epoch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public int $partition,
        public int $leaderEpoch,
        public int $endOffset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        $writer->writeInt16($this->errorCode);
        $writer->writeInt32($this->partition);
        if ($version >= 1) {
            $writer->writeInt32($this->leaderEpoch);
        }
        $writer->writeInt64($this->endOffset);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $errorCode = $reader->readInt16();
        $partition = $reader->readInt32();
        $leaderEpoch = -1;
        if ($version >= 1) {
            $leaderEpoch = $reader->readInt32();
        }
        $endOffset = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $partition, $leaderEpoch, $endOffset, $unknownTags);
    }
}
