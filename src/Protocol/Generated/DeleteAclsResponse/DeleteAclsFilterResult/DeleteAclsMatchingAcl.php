<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteAclsResponse\DeleteAclsFilterResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteAclsMatchingAcl implements Encodable, Decodable
{
    /**
     * @param int $errorCode The deletion error code, or 0 if the deletion succeeded.
     * @param ?string $errorMessage The deletion error message, or null if the deletion succeeded.
     * @param int $resourceType The ACL resource type.
     * @param string $resourceName The ACL resource name.
     * @param int $patternType The ACL resource pattern type.
     * @param string $principal The ACL principal.
     * @param string $host The ACL host.
     * @param int $operation The ACL operation.
     * @param int $permissionType The ACL permission type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public int $resourceType,
        public string $resourceName,
        public int $patternType,
        public string $principal,
        public string $host,
        public int $operation,
        public int $permissionType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
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
        if ($version >= 1) {
            $writer->writeInt8($this->patternType);
        }
        if ($flexible) {
            $writer->writeCompactString($this->principal);
        } else {
            $writer->writeString($this->principal);
        }
        if ($flexible) {
            $writer->writeCompactString($this->host);
        } else {
            $writer->writeString($this->host);
        }
        $writer->writeInt8($this->operation);
        $writer->writeInt8($this->permissionType);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
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
        $patternType = 3;
        if ($version >= 1) {
            $patternType = $reader->readInt8();
        }
        if ($flexible) {
            $principal = $reader->readCompactString();
        } else {
            $principal = $reader->readString();
        }
        if ($flexible) {
            $host = $reader->readCompactString();
        } else {
            $host = $reader->readString();
        }
        $operation = $reader->readInt8();
        $permissionType = $reader->readInt8();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $resourceType, $resourceName, $patternType, $principal, $host, $operation, $permissionType, $unknownTags);
    }
}
