<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeConfigsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeConfigsResource implements Encodable, Decodable
{
    /**
     * @param int $resourceType The resource type.
     * @param string $resourceName The resource name.
     * @param ?list<string> $configurationKeys The configuration keys to list, or null to list all configuration keys.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $resourceType,
        public string $resourceName,
        public ?array $configurationKeys,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        $writer->writeInt8($this->resourceType);
        if ($flexible) {
            $writer->writeCompactString($this->resourceName);
        } else {
            $writer->writeString($this->resourceName);
        }
        if ($flexible) {
            $writer->writeCompactNullableArrayLength($this->configurationKeys === null ? null : \count($this->configurationKeys));
        } else {
            $writer->writeNullableArrayLength($this->configurationKeys === null ? null : \count($this->configurationKeys));
        }
        if ($this->configurationKeys !== null) {
            foreach ($this->configurationKeys as $item) {
                if ($flexible) {
                    $writer->writeCompactString($item);
                } else {
                    $writer->writeString($item);
                }
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
        $resourceType = $reader->readInt8();
        if ($flexible) {
            $resourceName = $reader->readCompactString();
        } else {
            $resourceName = $reader->readString();
        }
        if ($flexible) {
            $configurationKeysCount = $reader->readCompactNullableArrayLength();
        } else {
            $configurationKeysCount = $reader->readNullableArrayLength();
        }
        if ($configurationKeysCount === null) {
            $configurationKeys = null;
        } else {
            $configurationKeys = [];
            for ($configurationKeysIndex = 0; $configurationKeysIndex < $configurationKeysCount; ++$configurationKeysIndex) {
                if ($flexible) {
                    $item = $reader->readCompactString();
                } else {
                    $item = $reader->readString();
                }
                $configurationKeys[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($resourceType, $resourceName, $configurationKeys, $unknownTags);
    }
}
