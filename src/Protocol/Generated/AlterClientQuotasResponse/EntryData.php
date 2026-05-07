<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterClientQuotasResponse;

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
     * @param int $errorCode The error code, or `0` if the quota alteration succeeded.
     * @param ?string $errorMessage The error message, or `null` if the quota alteration succeeded.
     * @param list<EntryData\EntityData> $entity The quota entity to alter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public array $entity,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeCompactNullableString($this->errorMessage);
        } else {
            $writer->writeNullableString($this->errorMessage);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->entity));
        } else {
            $writer->writeArrayLength(\count($this->entity));
        }
        foreach ($this->entity as $item) {
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
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $errorMessage = $reader->readCompactNullableString();
        } else {
            $errorMessage = $reader->readNullableString();
        }
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
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $entity, $unknownTags);
    }
}
