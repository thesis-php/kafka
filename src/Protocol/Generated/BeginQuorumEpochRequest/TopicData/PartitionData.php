<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\BeginQuorumEpochRequest\TopicData;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
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
     * @param int $partitionIndex The partition index.
     * @param UuidInterface $voterDirectoryId The directory id of the receiving replica.
     * @param int $leaderId The ID of the newly elected leader.
     * @param int $leaderEpoch The epoch of the newly elected leader.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public UuidInterface $voterDirectoryId,
        public int $leaderId,
        public int $leaderEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt32($this->partitionIndex);
        if ($version >= 1) {
            $writer->writeUuid($this->voterDirectoryId);
        }
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $partitionIndex = $reader->readInt32();
        $voterDirectoryId = Uuid::fromString(Uuid::NIL);
        if ($version >= 1) {
            $voterDirectoryId = $reader->readUuid();
        }
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $voterDirectoryId, $leaderId, $leaderEpoch, $unknownTags);
    }
}
