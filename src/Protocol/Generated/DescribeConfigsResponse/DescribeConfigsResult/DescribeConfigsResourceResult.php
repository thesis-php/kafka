<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeConfigsResponse\DescribeConfigsResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeConfigsResourceResult implements Encodable, Decodable
{
    /**
     * @param string $name The configuration name.
     * @param ?string $value The configuration value.
     * @param bool $readOnly True if the configuration is read-only.
     * @param int $configSource The configuration source.
     * @param bool $isSensitive True if this configuration is sensitive.
     * @param list<DescribeConfigsResourceResult\DescribeConfigsSynonym> $synonyms The synonyms for this configuration key.
     * @param int $configType The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
     * @param ?string $documentation The configuration documentation.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public ?string $value,
        public bool $readOnly,
        public int $configSource,
        public bool $isSensitive,
        public array $synonyms,
        public int $configType,
        public ?string $documentation,
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
        $writer->writeBool($this->readOnly);
        if ($version >= 1) {
            $writer->writeInt8($this->configSource);
        }
        $writer->writeBool($this->isSensitive);
        if ($version >= 1) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->synonyms));
            } else {
                $writer->writeArrayLength(\count($this->synonyms));
            }
            foreach ($this->synonyms as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 3) {
            $writer->writeInt8($this->configType);
        }
        if ($version >= 3) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->documentation);
            } else {
                $writer->writeNullableString($this->documentation);
            }
        }
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
        $readOnly = $reader->readBool();
        $configSource = -1;
        if ($version >= 1) {
            $configSource = $reader->readInt8();
        }
        $isSensitive = $reader->readBool();
        $synonyms = [];
        if ($version >= 1) {
            if ($flexible) {
                $synonymsCount = $reader->readCompactArrayLength();
            } else {
                $synonymsCount = $reader->readArrayLength();
            }
            $synonyms = [];
            for ($synonymsIndex = 0; $synonymsIndex < $synonymsCount; ++$synonymsIndex) {
                $item = DescribeConfigsResourceResult\DescribeConfigsSynonym::decode($reader, $version);
                $synonyms[] = $item;
            }
        }
        $configType = 0;
        if ($version >= 3) {
            $configType = $reader->readInt8();
        }
        $documentation = null;
        if ($version >= 3) {
            if ($flexible) {
                $documentation = $reader->readCompactNullableString();
            } else {
                $documentation = $reader->readNullableString();
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $value, $readOnly, $configSource, $isSensitive, $synonyms, $configType, $documentation, $unknownTags);
    }
}
