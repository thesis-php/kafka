<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\VoteResponse\TopicData;

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
     * @param int $errorCode The partition level error code.
     * @param int $leaderId The ID of the current leader or -1 if the leader is unknown.
     * @param int $leaderEpoch The latest known leader epoch.
     * @param bool $voteGranted True if the vote was granted and false otherwise.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public int $leaderId,
        public int $leaderEpoch,
        public bool $voteGranted,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeBool($this->voteGranted);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $voteGranted = $reader->readBool();
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $leaderId, $leaderEpoch, $voteGranted, $unknownTags);
    }
}
