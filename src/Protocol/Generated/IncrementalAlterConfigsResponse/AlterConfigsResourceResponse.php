<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\IncrementalAlterConfigsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AlterConfigsResourceResponse implements Encodable, Decodable
{
    /**
     * @param int $errorCode The resource error code.
     * @param ?string $errorMessage The resource error message, or null if there was no error.
     * @param int $resourceType The resource type.
     * @param string $resourceName The resource name.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public int $resourceType,
        public string $resourceName,
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
        $writer->writeInt8($this->resourceType);
        if ($flexible) {
            $writer->writeCompactString($this->resourceName);
        } else {
            $writer->writeString($this->resourceName);
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
        $resourceType = $reader->readInt8();
        if ($flexible) {
            $resourceName = $reader->readCompactString();
        } else {
            $resourceName = $reader->readString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $resourceType, $resourceName, $unknownTags);
    }
}
