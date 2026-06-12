<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\LeaderChangeMessage;

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
final readonly class Voter implements Encodable, Decodable
{
    /**
     * @param int $voterId The ID of the voter.
     * @param UuidInterface $voterDirectoryId The directory id of the voter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $voterId,
        public UuidInterface $voterDirectoryId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->voterId);
        if ($version >= 1) {
            $writer->writeUuid($this->voterDirectoryId);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $voterId = $reader->readInt32();
        $voterDirectoryId = Uuid::fromString(Uuid::NIL);
        if ($version >= 1) {
            $voterDirectoryId = $reader->readUuid();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($voterId, $voterDirectoryId, $unknownTags);
    }
}
