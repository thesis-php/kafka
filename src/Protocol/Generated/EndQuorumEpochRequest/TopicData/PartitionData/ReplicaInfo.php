<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\EndQuorumEpochRequest\TopicData\PartitionData;

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
final readonly class ReplicaInfo implements Encodable, Decodable
{
    /**
     * @param int $candidateId The ID of the candidate replica.
     * @param UuidInterface $candidateDirectoryId The directory ID of the candidate replica.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $candidateId,
        public UuidInterface $candidateDirectoryId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt32($this->candidateId);
        $writer->writeUuid($this->candidateDirectoryId);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $candidateId = $reader->readInt32();
        $candidateDirectoryId = $reader->readUuid();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($candidateId, $candidateDirectoryId, $unknownTags);
    }
}
