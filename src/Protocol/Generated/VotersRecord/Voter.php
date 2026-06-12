<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\VotersRecord;

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
final readonly class Voter implements Encodable, Decodable
{
    /**
     * @param int $voterId The replica id of the voter in the topic partition.
     * @param UuidInterface $voterDirectoryId The directory id of the voter in the topic partition.
     * @param list<Voter\Endpoint> $endpoints The endpoint that can be used to communicate with the voter.
     * @param Voter\KRaftVersionFeature $kRaftVersionFeature The range of versions of the protocol that the replica supports.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $voterId,
        public UuidInterface $voterDirectoryId,
        public array $endpoints,
        public Voter\KRaftVersionFeature $kRaftVersionFeature,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->voterId);
        $writer->writeUuid($this->voterDirectoryId);
        $writer->writeCompactArrayLength(\count($this->endpoints));
        foreach ($this->endpoints as $item) {
            $item->encode($writer, $version);
        }
        $this->kRaftVersionFeature->encode($writer, $version);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $voterId = $reader->readInt32();
        $voterDirectoryId = $reader->readUuid();
        $endpointsCount = $reader->readCompactArrayLength();
        $endpoints = [];
        for ($endpointsIndex = 0; $endpointsIndex < $endpointsCount; ++$endpointsIndex) {
            $item = Voter\Endpoint::decode($reader, $version);
            $endpoints[] = $item;
        }
        $kRaftVersionFeature = Voter\KRaftVersionFeature::decode($reader, $version);
        $unknownTags = $reader->readTaggedFields();
        return new self($voterId, $voterDirectoryId, $endpoints, $kRaftVersionFeature, $unknownTags);
    }
}
