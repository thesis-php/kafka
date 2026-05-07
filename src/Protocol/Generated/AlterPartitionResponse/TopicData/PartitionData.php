<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionResponse\TopicData;

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
     * @param int $leaderId The broker ID of the leader.
     * @param int $leaderEpoch The leader epoch.
     * @param list<int> $isr The in-sync replica IDs.
     * @param int $leaderRecoveryState 1 if the partition is recovering from an unclean leader election; 0 otherwise.
     * @param int $partitionEpoch The current epoch for the partition for KRaft controllers.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public int $leaderId,
        public int $leaderEpoch,
        public array $isr,
        public int $leaderRecoveryState,
        public int $partitionEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeCompactArrayLength(\count($this->isr));
        foreach ($this->isr as $item) {
            $writer->writeInt32($item);
        }
        if ($version >= 1) {
            $writer->writeInt8($this->leaderRecoveryState);
        }
        $writer->writeInt32($this->partitionEpoch);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $isrCount = $reader->readCompactArrayLength();
        $isr = [];
        for ($isrIndex = 0; $isrIndex < $isrCount; ++$isrIndex) {
            $item = $reader->readInt32();
            $isr[] = $item;
        }
        $leaderRecoveryState = 0;
        if ($version >= 1) {
            $leaderRecoveryState = $reader->readInt8();
        }
        $partitionEpoch = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $leaderId, $leaderEpoch, $isr, $leaderRecoveryState, $partitionEpoch, $unknownTags);
    }
}
