<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateTopicsResponse\CreatableTopicResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatableTopicConfigs implements Encodable, Decodable
{
    /**
     * @param string $name The configuration name.
     * @param ?string $value The configuration value.
     * @param bool $readOnly True if the configuration is read-only.
     * @param int $configSource The configuration source.
     * @param bool $isSensitive True if this configuration is sensitive.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public ?string $value,
        public bool $readOnly,
        public int $configSource,
        public bool $isSensitive,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        $writer->writeCompactString($this->name);
        $writer->writeCompactNullableString($this->value);
        $writer->writeBool($this->readOnly);
        $writer->writeInt8($this->configSource);
        $writer->writeBool($this->isSensitive);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        $name = $reader->readCompactString();
        $value = $reader->readCompactNullableString();
        $readOnly = $reader->readBool();
        $configSource = $reader->readInt8();
        $isSensitive = $reader->readBool();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $value, $readOnly, $configSource, $isSensitive, $unknownTags);
    }
}
