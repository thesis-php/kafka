<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeConfigsResponse\DescribeConfigsResult\DescribeConfigsResourceResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeConfigsSynonym implements Encodable, Decodable
{
    /**
     * @param string $name The synonym name.
     * @param ?string $value The synonym value.
     * @param int $source The synonym source.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public ?string $value,
        public int $source,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
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
        $writer->writeInt8($this->source);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
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
        $source = $reader->readInt8();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $value, $source, $unknownTags);
    }
}
