<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateTopicsRequest\CreatableTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatableTopicConfig implements Encodable, Decodable
{
    /**
     * @param string $name The configuration name.
     * @param ?string $value The configuration value.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public ?string $value,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->value);
        } else {
            $writer->writeNullableString($this->value);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        if ($flexible) {
            $value = $reader->readCompactNullableString();
        } else {
            $value = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $value, $unknownTags);
    }
}
