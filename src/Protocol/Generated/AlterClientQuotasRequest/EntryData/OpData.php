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
final readonly class OpData implements Encodable, Decodable
{
    /**
     * @param string $key The quota configuration key.
     * @param float $value The value to set, otherwise ignored if the value is to be removed.
     * @param bool $remove Whether the quota configuration value should be removed, otherwise set.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $key,
        public float $value,
        public bool $remove,
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
        $writer->writeBool($this->remove);
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
        $remove = $reader->readBool();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($key, $value, $remove, $unknownTags);
    }
}
