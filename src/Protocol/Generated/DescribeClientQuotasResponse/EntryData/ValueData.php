<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeClientQuotasResponse\EntryData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ValueData implements Encodable, Decodable
{
    /**
     * @param string $key The quota configuration key.
     * @param float $value The quota configuration value.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $key,
        public float $value,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactString($this->key);
        } else {
            $writer->writeString($this->key);
        }
        $writer->writeFloat64($this->value);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $key = $reader->readCompactString();
        } else {
            $key = $reader->readString();
        }
        $value = $reader->readFloat64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($key, $value, $unknownTags);
    }
}
