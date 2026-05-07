<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListOffsetsResponse\ListOffsetsTopicResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ListOffsetsPartitionResponse implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $errorCode The partition error code, or 0 if there was no error.
     * @param int $timestamp The timestamp associated with the returned offset.
     * @param int $offset The returned offset.
     * @param int $leaderEpoch The leader epoch associated with the returned offset.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public int $timestamp,
        public int $offset,
        public int $leaderEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        if ($version >= 1) {
            $writer->writeInt64($this->timestamp);
        }
        if ($version >= 1) {
            $writer->writeInt64($this->offset);
        }
        if ($version >= 4) {
            $writer->writeInt32($this->leaderEpoch);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $timestamp = -1;
        if ($version >= 1) {
            $timestamp = $reader->readInt64();
        }
        $offset = -1;
        if ($version >= 1) {
            $offset = $reader->readInt64();
        }
        $leaderEpoch = -1;
        if ($version >= 4) {
            $leaderEpoch = $reader->readInt32();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $errorCode, $timestamp, $offset, $leaderEpoch, $unknownTags);
    }
}
