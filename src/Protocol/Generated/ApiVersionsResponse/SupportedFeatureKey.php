<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ApiVersionsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class SupportedFeatureKey implements Encodable, Decodable
{
    /**
     * @param string $name The name of the feature.
     * @param int $minVersion The minimum supported version for the feature.
     * @param int $maxVersion The maximum supported version for the feature.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $minVersion,
        public int $maxVersion,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeCompactString($this->name);
        $writer->writeInt16($this->minVersion);
        $writer->writeInt16($this->maxVersion);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $name = $reader->readCompactString();
        $minVersion = $reader->readInt16();
        $maxVersion = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $minVersion, $maxVersion, $unknownTags);
    }
}
