<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeClientQuotasResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class EntryData implements Encodable, Decodable
{
    /**
     * @param list<EntryData\EntityData> $entity The quota entity description.
     * @param list<EntryData\ValueData> $values The quota values for the entity.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $entity,
        public array $values,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->entity));
        } else {
            $writer->writeArrayLength(\count($this->entity));
        }
        foreach ($this->entity as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->values));
        } else {
            $writer->writeArrayLength(\count($this->values));
        }
        foreach ($this->values as $item) {
            $item->encode($writer, $version);
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
            $entityCount = $reader->readCompactArrayLength();
        } else {
            $entityCount = $reader->readArrayLength();
        }
        $entity = [];
        for ($entityIndex = 0; $entityIndex < $entityCount; ++$entityIndex) {
            $item = EntryData\EntityData::decode($reader, $version);
            $entity[] = $item;
        }
        if ($flexible) {
            $valuesCount = $reader->readCompactArrayLength();
        } else {
            $valuesCount = $reader->readArrayLength();
        }
        $values = [];
        for ($valuesIndex = 0; $valuesIndex < $valuesCount; ++$valuesIndex) {
            $item = EntryData\ValueData::decode($reader, $version);
            $values[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($entity, $values, $unknownTags);
    }
}
