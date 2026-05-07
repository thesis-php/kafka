<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListConfigResourcesResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ConfigResource implements Encodable, Decodable
{
    /**
     * @param string $resourceName The resource name.
     * @param int $resourceType The resource type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $resourceName,
        public int $resourceType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->resourceName);
        if ($version >= 1) {
            $writer->writeInt8($this->resourceType);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $resourceName = $reader->readCompactString();
        $resourceType = 16;
        if ($version >= 1) {
            $resourceType = $reader->readInt8();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($resourceName, $resourceType, $unknownTags);
    }
}
