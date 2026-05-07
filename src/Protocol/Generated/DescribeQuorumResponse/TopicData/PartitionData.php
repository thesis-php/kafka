<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeQuorumResponse\TopicData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\DescribeQuorumResponse\ReplicaState;
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
     * @param int $errorCode The partition error code.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param int $leaderId The ID of the current leader or -1 if the leader is unknown.
     * @param int $leaderEpoch The latest known leader epoch.
     * @param int $highWatermark The high water mark.
     * @param list<ReplicaState> $currentVoters The current voters of the partition.
     * @param list<ReplicaState> $observers The observers of the partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public ?string $errorMessage,
        public int $leaderId,
        public int $leaderEpoch,
        public int $highWatermark,
        public array $currentVoters,
        public array $observers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        if ($version >= 2) {
            $writer->writeCompactNullableString($this->errorMessage);
        }
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeInt64($this->highWatermark);
        $writer->writeCompactArrayLength(\count($this->currentVoters));
        foreach ($this->currentVoters as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->observers));
        foreach ($this->observers as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = null;
        if ($version >= 2) {
            $errorMessage = $reader->readCompactNullableString();
        }
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $highWatermark = $reader->readInt64();
        $currentVotersCount = $reader->readCompactArrayLength();
        $currentVoters = [];
        for ($currentVotersIndex = 0; $currentVotersIndex < $currentVotersCount; ++$currentVotersIndex) {
            $item = ReplicaState::decode($reader, $version);
            $currentVoters[] = $item;
        }
        $observersCount = $reader->readCompactArrayLength();
        $observers = [];
        for ($observersIndex = 0; $observersIndex < $observersCount; ++$observersIndex) {
            $item = ReplicaState::decode($reader, $version);
            $observers[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $errorMessage, $leaderId, $leaderEpoch, $highWatermark, $currentVoters, $observers, $unknownTags);
    }
}
