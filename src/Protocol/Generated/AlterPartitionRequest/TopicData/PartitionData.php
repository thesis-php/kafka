<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionRequest\TopicData;

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
     * @param int $leaderEpoch The leader epoch of this partition.
     * @param list<int> $newIsr The ISR for this partition. Deprecated since version 3.
     * @param list<PartitionData\BrokerState> $newIsrWithEpochs The ISR for this partition.
     * @param int $leaderRecoveryState 1 if the partition is recovering from an unclean leader election; 0 otherwise.
     * @param int $partitionEpoch The expected epoch of the partition which is being updated.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $leaderEpoch,
        public array $newIsr,
        public array $newIsrWithEpochs,
        public int $leaderRecoveryState,
        public int $partitionEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt32($this->leaderEpoch);
        if ($version <= 2) {
            $writer->writeCompactArrayLength(\count($this->newIsr));
            foreach ($this->newIsr as $item) {
                $writer->writeInt32($item);
            }
        }
        if ($version >= 3) {
            $writer->writeCompactArrayLength(\count($this->newIsrWithEpochs));
            foreach ($this->newIsrWithEpochs as $item) {
                $item->encode($writer, $version);
            }
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
        $leaderEpoch = $reader->readInt32();
        $newIsr = [];
        if ($version <= 2) {
            $newIsrCount = $reader->readCompactArrayLength();
            $newIsr = [];
            for ($newIsrIndex = 0; $newIsrIndex < $newIsrCount; ++$newIsrIndex) {
                $item = $reader->readInt32();
                $newIsr[] = $item;
            }
        }
        $newIsrWithEpochs = [];
        if ($version >= 3) {
            $newIsrWithEpochsCount = $reader->readCompactArrayLength();
            $newIsrWithEpochs = [];
            for ($newIsrWithEpochsIndex = 0; $newIsrWithEpochsIndex < $newIsrWithEpochsCount; ++$newIsrWithEpochsIndex) {
                $item = PartitionData\BrokerState::decode($reader, $version);
                $newIsrWithEpochs[] = $item;
            }
        }
        $leaderRecoveryState = 0;
        if ($version >= 1) {
            $leaderRecoveryState = $reader->readInt8();
        }
        $partitionEpoch = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $leaderEpoch, $newIsr, $newIsrWithEpochs, $leaderRecoveryState, $partitionEpoch, $unknownTags);
    }
}
