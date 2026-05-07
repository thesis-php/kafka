<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeClientQuotasRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ComponentData implements Encodable, Decodable
{
    /**
     * @param string $entityType The entity type that the filter component applies to.
     * @param int $matchType How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
     * @param ?string $match The string to match against, or null if unused for the match type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $entityType,
        public int $matchType,
        public ?string $match,
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
        $writer->writeInt8($this->matchType);
        if ($flexible) {
            $writer->writeCompactNullableString($this->match);
        } else {
            $writer->writeNullableString($this->match);
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
        $matchType = $reader->readInt8();
        if ($flexible) {
            $match = $reader->readCompactNullableString();
        } else {
            $match = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($entityType, $matchType, $match, $unknownTags);
    }
}
