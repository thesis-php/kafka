<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchRequest\FetchTopic;

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
final readonly class FetchPartition implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $currentLeaderEpoch The current leader epoch of the partition.
     * @param int $fetchOffset The message offset.
     * @param int $lastFetchedEpoch The epoch of the last fetched record or -1 if there is none.
     * @param int $logStartOffset The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
     * @param int $partitionMaxBytes The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
     * @param UuidInterface $replicaDirectoryId The directory id of the follower fetching.
     * @param int $highWatermark The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $currentLeaderEpoch,
        public int $fetchOffset,
        public int $lastFetchedEpoch,
        public int $logStartOffset,
        public int $partitionMaxBytes,
        public UuidInterface $replicaDirectoryId,
        public int $highWatermark,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt32($this->partition);
        if ($version >= 9) {
            $writer->writeInt32($this->currentLeaderEpoch);
        }
        $writer->writeInt64($this->fetchOffset);
        if ($version >= 12) {
            $writer->writeInt32($this->lastFetchedEpoch);
        }
        if ($version >= 5) {
            $writer->writeInt64($this->logStartOffset);
        }
        $writer->writeInt32($this->partitionMaxBytes);
        if ($flexible) {
            $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
            if ($version >= 17) {
                if (isset($taggedFields[0])) {
                    throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'ReplicaDirectoryId\'.');
                }
                $tagWriter = new Writer();
                $tagWriter->writeUuid($this->replicaDirectoryId);
                $taggedFields[0] = (string) $tagWriter;
            }
            if ($version >= 18) {
                if (isset($taggedFields[1])) {
                    throw new \UnexpectedValueException('Unknown tag \'1\' collides with known field \'HighWatermark\'.');
                }
                $tagWriter = new Writer();
                $tagWriter->writeInt64($this->highWatermark);
                $taggedFields[1] = (string) $tagWriter;
            }
            $writer->writeTaggedFields($taggedFields);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $partition = $reader->readInt32();
        $currentLeaderEpoch = -1;
        if ($version >= 9) {
            $currentLeaderEpoch = $reader->readInt32();
        }
        $fetchOffset = $reader->readInt64();
        $lastFetchedEpoch = -1;
        if ($version >= 12) {
            $lastFetchedEpoch = $reader->readInt32();
        }
        $logStartOffset = -1;
        if ($version >= 5) {
            $logStartOffset = $reader->readInt64();
        }
        $partitionMaxBytes = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if ($version >= 17) {
                if (isset($unknownTags[0])) {
                    $tagReader = new Reader($unknownTags[0]);
                    $replicaDirectoryId = $tagReader->readUuid();
                    unset($unknownTags[0]);
                } else {
                    $replicaDirectoryId = Uuid::fromString(Uuid::NIL);
                }
            } else {
                $replicaDirectoryId = Uuid::fromString(Uuid::NIL);
            }
            if ($version >= 18) {
                if (isset($unknownTags[1])) {
                    $tagReader = new Reader($unknownTags[1]);
                    $highWatermark = $tagReader->readInt64();
                    unset($unknownTags[1]);
                } else {
                    $highWatermark = 9223372036854775807;
                }
            } else {
                $highWatermark = 9223372036854775807;
            }
        } else {
            $unknownTags = new UnknownTags();
            $replicaDirectoryId = Uuid::fromString(Uuid::NIL);
            $highWatermark = 9223372036854775807;
        }
        return new self($partition, $currentLeaderEpoch, $fetchOffset, $lastFetchedEpoch, $logStartOffset, $partitionMaxBytes, $replicaDirectoryId, $highWatermark, $unknownTags);
    }
}
