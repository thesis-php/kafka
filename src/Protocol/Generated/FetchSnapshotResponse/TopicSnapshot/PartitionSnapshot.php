<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchSnapshotResponse\TopicSnapshot;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionSnapshot implements Encodable, Decodable
{
    /**
     * @param int $index The partition index.
     * @param int $errorCode The error code, or 0 if there was no fetch error.
     * @param PartitionSnapshot\SnapshotId $snapshotId The snapshot endOffset and epoch fetched.
     * @param PartitionSnapshot\LeaderIdAndEpoch $currentLeader The leader of the partition at the time of the snapshot.
     * @param int $size The total size of the snapshot.
     * @param int $position The starting byte position within the snapshot included in the Bytes field.
     * @param string $unalignedRecords Snapshot data in records format which may not be aligned on an offset boundary.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $index,
        public int $errorCode,
        public PartitionSnapshot\SnapshotId $snapshotId,
        public PartitionSnapshot\LeaderIdAndEpoch $currentLeader,
        public int $size,
        public int $position,
        public string $unalignedRecords,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->index);
        $writer->writeInt16($this->errorCode);
        $this->snapshotId->encode($writer, $version);
        $writer->writeInt64($this->size);
        $writer->writeInt64($this->position);
        $writer->writeCompactBytes($this->unalignedRecords);
        $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
        if (isset($taggedFields[0])) {
            throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'CurrentLeader\'.');
        }
        $tagWriter = new Writer();
        $this->currentLeader->encode($tagWriter, $version);
        $taggedFields[0] = (string) $tagWriter;
        $writer->writeTaggedFields($taggedFields);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $index = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $snapshotId = PartitionSnapshot\SnapshotId::decode($reader, $version);
        $size = $reader->readInt64();
        $position = $reader->readInt64();
        $unalignedRecords = $reader->readCompactBytes();
        $unknownTags = $reader->readTaggedFields();
        if (isset($unknownTags[0])) {
            $tagReader = new Reader($unknownTags[0]);
            $currentLeader = PartitionSnapshot\LeaderIdAndEpoch::decode($tagReader, $version);
            unset($unknownTags[0]);
        } else {
            $currentLeader = new PartitionSnapshot\LeaderIdAndEpoch(0, 0);
        }
        return new self($index, $errorCode, $snapshotId, $currentLeader, $size, $position, $unalignedRecords, $unknownTags);
    }
}
