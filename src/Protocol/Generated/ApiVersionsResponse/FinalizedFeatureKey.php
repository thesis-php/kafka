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
final readonly class FinalizedFeatureKey implements Encodable, Decodable
{
    /**
     * @param string $name The name of the feature.
     * @param int $maxVersionLevel The cluster-wide finalized max version level for the feature.
     * @param int $minVersionLevel The cluster-wide finalized min version level for the feature.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $maxVersionLevel,
        public int $minVersionLevel,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeCompactString($this->name);
        $writer->writeInt16($this->maxVersionLevel);
        $writer->writeInt16($this->minVersionLevel);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $name = $reader->readCompactString();
        $maxVersionLevel = $reader->readInt16();
        $minVersionLevel = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $maxVersionLevel, $minVersionLevel, $unknownTags);
    }
}
