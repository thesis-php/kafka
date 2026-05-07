<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteAclsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteAclsFilter implements Encodable, Decodable
{
    /**
     * @param int $resourceTypeFilter The resource type.
     * @param ?string $resourceNameFilter The resource name, or null to match any resource name.
     * @param int $patternTypeFilter The pattern type.
     * @param ?string $principalFilter The principal filter, or null to accept all principals.
     * @param ?string $hostFilter The host filter, or null to accept all hosts.
     * @param int $operation The ACL operation.
     * @param int $permissionType The permission type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $resourceTypeFilter,
        public ?string $resourceNameFilter,
        public int $patternTypeFilter,
        public ?string $principalFilter,
        public ?string $hostFilter,
        public int $operation,
        public int $permissionType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt8($this->resourceTypeFilter);
        if ($flexible) {
            $writer->writeCompactNullableString($this->resourceNameFilter);
        } else {
            $writer->writeNullableString($this->resourceNameFilter);
        }
        if ($version >= 1) {
            $writer->writeInt8($this->patternTypeFilter);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->principalFilter);
        } else {
            $writer->writeNullableString($this->principalFilter);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->hostFilter);
        } else {
            $writer->writeNullableString($this->hostFilter);
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
        $resourceTypeFilter = $reader->readInt8();
        if ($flexible) {
            $resourceNameFilter = $reader->readCompactNullableString();
        } else {
            $resourceNameFilter = $reader->readNullableString();
        }
        $patternTypeFilter = 3;
        if ($version >= 1) {
            $patternTypeFilter = $reader->readInt8();
        }
        if ($flexible) {
            $principalFilter = $reader->readCompactNullableString();
        } else {
            $principalFilter = $reader->readNullableString();
        }
        if ($flexible) {
            $hostFilter = $reader->readCompactNullableString();
        } else {
            $hostFilter = $reader->readNullableString();
        }
        $operation = $reader->readInt8();
        $permissionType = $reader->readInt8();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($resourceTypeFilter, $resourceNameFilter, $patternTypeFilter, $principalFilter, $hostFilter, $operation, $permissionType, $unknownTags);
    }
}
