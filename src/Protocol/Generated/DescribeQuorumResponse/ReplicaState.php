<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeQuorumResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ReplicaState implements Encodable, Decodable
{
    /**
     * @param int $replicaId The ID of the replica.
     * @param string $replicaDirectoryId The replica directory ID of the replica.
     * @param int $logEndOffset The last known log end offset of the follower or -1 if it is unknown.
     * @param int $lastFetchTimestamp The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter.
     * @param int $lastCaughtUpTimestamp The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $replicaId,
        public string $replicaDirectoryId,
        public int $logEndOffset,
        public int $lastFetchTimestamp,
        public int $lastCaughtUpTimestamp,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->replicaId);
        if ($version >= 2) {
            $writer->writeUuid($this->replicaDirectoryId);
        }
        $writer->writeInt64($this->logEndOffset);
        if ($version >= 1) {
            $writer->writeInt64($this->lastFetchTimestamp);
        }
        if ($version >= 1) {
            $writer->writeInt64($this->lastCaughtUpTimestamp);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $replicaId = $reader->readInt32();
        $replicaDirectoryId = '';
        if ($version >= 2) {
            $replicaDirectoryId = $reader->readUuid();
        }
        $logEndOffset = $reader->readInt64();
        $lastFetchTimestamp = -1;
        if ($version >= 1) {
            $lastFetchTimestamp = $reader->readInt64();
        }
        $lastCaughtUpTimestamp = -1;
        if ($version >= 1) {
            $lastCaughtUpTimestamp = $reader->readInt64();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($replicaId, $replicaDirectoryId, $logEndOffset, $lastFetchTimestamp, $lastCaughtUpTimestamp, $unknownTags);
    }
}
