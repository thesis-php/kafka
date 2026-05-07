<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\WriteShareGroupStateRequest\WriteStateData;

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
     * @param int $partition The partition index.
     * @param int $stateEpoch The state epoch of the share-partition.
     * @param int $leaderEpoch The leader epoch of the share-partition.
     * @param int $startOffset The share-partition start offset, or -1 if the start offset is not being written.
     * @param int $deliveryCompleteCount The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
     * @param list<PartitionData\StateBatch> $stateBatches The state batches for the share-partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $stateEpoch,
        public int $leaderEpoch,
        public int $startOffset,
        public int $deliveryCompleteCount,
        public array $stateBatches,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt32($this->stateEpoch);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeInt64($this->startOffset);
        if ($version >= 1) {
            $writer->writeInt32($this->deliveryCompleteCount);
        }
        $writer->writeCompactArrayLength(\count($this->stateBatches));
        foreach ($this->stateBatches as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $stateEpoch = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $startOffset = $reader->readInt64();
        $deliveryCompleteCount = -1;
        if ($version >= 1) {
            $deliveryCompleteCount = $reader->readInt32();
        }
        $stateBatchesCount = $reader->readCompactArrayLength();
        $stateBatches = [];
        for ($stateBatchesIndex = 0; $stateBatchesIndex < $stateBatchesCount; ++$stateBatchesIndex) {
            $item = PartitionData\StateBatch::decode($reader, $version);
            $stateBatches[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $stateEpoch, $leaderEpoch, $startOffset, $deliveryCompleteCount, $stateBatches, $unknownTags);
    }
}
