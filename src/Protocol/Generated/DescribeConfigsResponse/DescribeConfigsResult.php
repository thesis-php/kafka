<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeConfigsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeConfigsResult implements Encodable, Decodable
{
    /**
     * @param int $errorCode The error code, or 0 if we were able to successfully describe the configurations.
     * @param ?string $errorMessage The error message, or null if we were able to successfully describe the configurations.
     * @param int $resourceType The resource type.
     * @param string $resourceName The resource name.
     * @param list<DescribeConfigsResult\DescribeConfigsResourceResult> $configs Each listed configuration.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public int $resourceType,
        public string $resourceName,
        public array $configs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeCompactNullableString($this->errorMessage);
        } else {
            $writer->writeNullableString($this->errorMessage);
        }
        $writer->writeInt8($this->resourceType);
        if ($flexible) {
            $writer->writeCompactString($this->resourceName);
        } else {
            $writer->writeString($this->resourceName);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->configs));
        } else {
            $writer->writeArrayLength(\count($this->configs));
        }
        foreach ($this->configs as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $errorMessage = $reader->readCompactNullableString();
        } else {
            $errorMessage = $reader->readNullableString();
        }
        $resourceType = $reader->readInt8();
        if ($flexible) {
            $resourceName = $reader->readCompactString();
        } else {
            $resourceName = $reader->readString();
        }
        if ($flexible) {
            $configsCount = $reader->readCompactArrayLength();
        } else {
            $configsCount = $reader->readArrayLength();
        }
        $configs = [];
        for ($configsIndex = 0; $configsIndex < $configsCount; ++$configsIndex) {
            $item = DescribeConfigsResult\DescribeConfigsResourceResult::decode($reader, $version);
            $configs[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $resourceType, $resourceName, $configs, $unknownTags);
    }
}
