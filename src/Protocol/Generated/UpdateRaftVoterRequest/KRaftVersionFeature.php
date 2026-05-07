<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\UpdateRaftVoterRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class KRaftVersionFeature implements Encodable, Decodable
{
    /**
     * @param int $minSupportedVersion The minimum supported KRaft protocol version.
     * @param int $maxSupportedVersion The maximum supported KRaft protocol version.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $minSupportedVersion,
        public int $maxSupportedVersion,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->minSupportedVersion);
        $writer->writeInt16($this->maxSupportedVersion);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $minSupportedVersion = $reader->readInt16();
        $maxSupportedVersion = $reader->readInt16();
        $unknownTags = $reader->readTaggedFields();
        return new self($minSupportedVersion, $maxSupportedVersion, $unknownTags);
    }
}
