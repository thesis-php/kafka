<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeAclsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeAclsResource implements Encodable, Decodable
{
    /**
     * @param int $resourceType The resource type.
     * @param string $resourceName The resource name.
     * @param int $patternType The resource pattern type.
     * @param list<DescribeAclsResource\AclDescription> $acls The ACLs.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $resourceType,
        public string $resourceName,
        public int $patternType,
        public array $acls,
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
            $writer->writeInt8($this->patternType);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->acls));
        } else {
            $writer->writeArrayLength(\count($this->acls));
        }
        foreach ($this->acls as $item) {
            $item->encode($writer, $version);
        }
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
        $patternType = 3;
        if ($version >= 1) {
            $patternType = $reader->readInt8();
        }
        if ($flexible) {
            $aclsCount = $reader->readCompactArrayLength();
        } else {
            $aclsCount = $reader->readArrayLength();
        }
        $acls = [];
        for ($aclsIndex = 0; $aclsIndex < $aclsCount; ++$aclsIndex) {
            $item = DescribeAclsResource\AclDescription::decode($reader, $version);
            $acls[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($resourceType, $resourceName, $patternType, $acls, $unknownTags);
    }
}
