<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateAclsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AclCreation implements Encodable, Decodable
{
    /**
     * @param int $resourceType The type of the resource.
     * @param string $resourceName The resource name for the ACL.
     * @param int $resourcePatternType The pattern type for the ACL.
     * @param string $principal The principal for the ACL.
     * @param string $host The host for the ACL.
     * @param int $operation The operation type for the ACL (read, write, etc.).
     * @param int $permissionType The permission type for the ACL (allow, deny, etc.).
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $resourceType,
        public string $resourceName,
        public int $resourcePatternType,
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
        $writer->writeInt8($this->resourceType);
        if ($flexible) {
            $writer->writeCompactString($this->resourceName);
        } else {
            $writer->writeString($this->resourceName);
        }
        if ($version >= 1) {
            $writer->writeInt8($this->resourcePatternType);
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
        $resourceType = $reader->readInt8();
        if ($flexible) {
            $resourceName = $reader->readCompactString();
        } else {
            $resourceName = $reader->readString();
        }
        $resourcePatternType = 3;
        if ($version >= 1) {
            $resourcePatternType = $reader->readInt8();
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
        return new self($resourceType, $resourceName, $resourcePatternType, $principal, $host, $operation, $permissionType, $unknownTags);
    }
}
