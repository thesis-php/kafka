<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchSnapshotRequest\TopicSnapshot;

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
     * @param int $partition The partition index.
     * @param int $currentLeaderEpoch The current leader epoch of the partition, -1 for unknown leader epoch.
     * @param PartitionSnapshot\SnapshotId $snapshotId The snapshot endOffset and epoch to fetch.
     * @param int $position The byte position within the snapshot to start fetching from.
     * @param string $replicaDirectoryId The directory id of the follower fetching.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $currentLeaderEpoch,
        public PartitionSnapshot\SnapshotId $snapshotId,
        public int $position,
        public string $replicaDirectoryId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt32($this->currentLeaderEpoch);
        $this->snapshotId->encode($writer, $version);
        $writer->writeInt64($this->position);
        $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
        if ($version >= 1) {
            if (isset($taggedFields[0])) {
                throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'ReplicaDirectoryId\'.');
            }
            $tagWriter = new Writer();
            $tagWriter->writeUuid($this->replicaDirectoryId);
            $taggedFields[0] = (string) $tagWriter;
        }
        $writer->writeTaggedFields($taggedFields);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $currentLeaderEpoch = $reader->readInt32();
        $snapshotId = PartitionSnapshot\SnapshotId::decode($reader, $version);
        $position = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        if ($version >= 1) {
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $replicaDirectoryId = $tagReader->readUuid();
                unset($unknownTags[0]);
            } else {
                $replicaDirectoryId = '';
            }
        } else {
            $replicaDirectoryId = '';
        }
        return new self($partition, $currentLeaderEpoch, $snapshotId, $position, $replicaDirectoryId, $unknownTags);
    }
}
