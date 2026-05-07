<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\VoteRequest\TopicData;

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
     * @param int $replicaEpoch The epoch of the voter sending the request
     * @param int $replicaId The replica id of the voter sending the request
     * @param string $replicaDirectoryId The directory id of the voter sending the request
     * @param string $voterDirectoryId The directory id of the voter receiving the request
     * @param int $lastOffsetEpoch The epoch of the last record written to the metadata log.
     * @param int $lastOffset The log end offset of the metadata log of the voter sending the request.
     * @param bool $preVote Whether the request is a PreVote request (not persisted) or not.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $replicaEpoch,
        public int $replicaId,
        public string $replicaDirectoryId,
        public string $voterDirectoryId,
        public int $lastOffsetEpoch,
        public int $lastOffset,
        public bool $preVote,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt32($this->replicaEpoch);
        $writer->writeInt32($this->replicaId);
        if ($version >= 1) {
            $writer->writeUuid($this->replicaDirectoryId);
        }
        if ($version >= 1) {
            $writer->writeUuid($this->voterDirectoryId);
        }
        $writer->writeInt32($this->lastOffsetEpoch);
        $writer->writeInt64($this->lastOffset);
        if ($version >= 2) {
            $writer->writeBool($this->preVote);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $replicaEpoch = $reader->readInt32();
        $replicaId = $reader->readInt32();
        $replicaDirectoryId = '';
        if ($version >= 1) {
            $replicaDirectoryId = $reader->readUuid();
        }
        $voterDirectoryId = '';
        if ($version >= 1) {
            $voterDirectoryId = $reader->readUuid();
        }
        $lastOffsetEpoch = $reader->readInt32();
        $lastOffset = $reader->readInt64();
        $preVote = false;
        if ($version >= 2) {
            $preVote = $reader->readBool();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $replicaEpoch, $replicaId, $replicaDirectoryId, $voterDirectoryId, $lastOffsetEpoch, $lastOffset, $preVote, $unknownTags);
    }
}
