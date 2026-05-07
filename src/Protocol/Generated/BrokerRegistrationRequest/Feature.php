<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\BrokerRegistrationRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Feature implements Encodable, Decodable
{
    /**
     * @param string $name The feature name.
     * @param int $minSupportedVersion The minimum supported feature level.
     * @param int $maxSupportedVersion The maximum supported feature level.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $minSupportedVersion,
        public int $maxSupportedVersion,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeInt16($this->minSupportedVersion);
        $writer->writeInt16($this->maxSupportedVersion);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $minSupportedVersion = $reader->readInt16();
        $maxSupportedVersion = $reader->readInt16();
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $minSupportedVersion, $maxSupportedVersion, $unknownTags);
    }
}
