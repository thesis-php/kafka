<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterClientQuotasRequest\EntryData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class EntityData implements Encodable, Decodable
{
    /**
     * @param string $entityType The entity type.
     * @param ?string $entityName The name of the entity, or null if the default.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $entityType,
        public ?string $entityName,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactString($this->entityType);
        } else {
            $writer->writeString($this->entityType);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->entityName);
        } else {
            $writer->writeNullableString($this->entityName);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $entityType = $reader->readCompactString();
        } else {
            $entityType = $reader->readString();
        }
        if ($flexible) {
            $entityName = $reader->readCompactNullableString();
        } else {
            $entityName = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($entityType, $entityName, $unknownTags);
    }
}
