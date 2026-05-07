<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListOffsetsRequest\ListOffsetsTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ListOffsetsPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $currentLeaderEpoch The current leader epoch.
     * @param int $timestamp The current timestamp.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $currentLeaderEpoch,
        public int $timestamp,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeInt32($this->partitionIndex);
        if ($version >= 4) {
            $writer->writeInt32($this->currentLeaderEpoch);
        }
        $writer->writeInt64($this->timestamp);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $partitionIndex = $reader->readInt32();
        $currentLeaderEpoch = -1;
        if ($version >= 4) {
            $currentLeaderEpoch = $reader->readInt32();
        }
        $timestamp = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $currentLeaderEpoch, $timestamp, $unknownTags);
    }
}
