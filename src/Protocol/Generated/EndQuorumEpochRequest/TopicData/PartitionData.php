<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\EndQuorumEpochRequest\TopicData;

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
     * @param int $leaderId The current leader ID that is resigning.
     * @param int $leaderEpoch The current epoch.
     * @param list<int> $preferredSuccessors A sorted list of preferred successors to start the election.
     * @param list<PartitionData\ReplicaInfo> $preferredCandidates A sorted list of preferred candidates to start the election.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $leaderId,
        public int $leaderEpoch,
        public array $preferredSuccessors,
        public array $preferredCandidates,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        if ($version <= 0) {
            $writer->writeArrayLength(\count($this->preferredSuccessors));
            foreach ($this->preferredSuccessors as $item) {
                $writer->writeInt32($item);
            }
        }
        if ($version >= 1) {
            $writer->writeCompactArrayLength(\count($this->preferredCandidates));
            foreach ($this->preferredCandidates as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $partitionIndex = $reader->readInt32();
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $preferredSuccessors = [];
        if ($version <= 0) {
            $preferredSuccessorsCount = $reader->readArrayLength();
            $preferredSuccessors = [];
            for ($preferredSuccessorsIndex = 0; $preferredSuccessorsIndex < $preferredSuccessorsCount; ++$preferredSuccessorsIndex) {
                $item = $reader->readInt32();
                $preferredSuccessors[] = $item;
            }
        }
        $preferredCandidates = [];
        if ($version >= 1) {
            $preferredCandidatesCount = $reader->readCompactArrayLength();
            $preferredCandidates = [];
            for ($preferredCandidatesIndex = 0; $preferredCandidatesIndex < $preferredCandidatesCount; ++$preferredCandidatesIndex) {
                $item = PartitionData\ReplicaInfo::decode($reader, $version);
                $preferredCandidates[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $leaderId, $leaderEpoch, $preferredSuccessors, $preferredCandidates, $unknownTags);
    }
}
