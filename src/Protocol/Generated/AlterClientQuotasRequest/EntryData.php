<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterClientQuotasRequest;

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
     * @param list<EntryData\EntityData> $entity The quota entity to alter.
     * @param list<EntryData\OpData> $ops An individual quota configuration entry to alter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $entity,
        public array $ops,
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
            $writer->writeCompactArrayLength(\count($this->ops));
        } else {
            $writer->writeArrayLength(\count($this->ops));
        }
        foreach ($this->ops as $item) {
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
            $opsCount = $reader->readCompactArrayLength();
        } else {
            $opsCount = $reader->readArrayLength();
        }
        $ops = [];
        for ($opsIndex = 0; $opsIndex < $opsCount; ++$opsIndex) {
            $item = EntryData\OpData::decode($reader, $version);
            $ops[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($entity, $ops, $unknownTags);
    }
}
