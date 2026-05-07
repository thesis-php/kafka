<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeAclsResponse\DescribeAclsResource;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AclDescription implements Encodable, Decodable
{
    /**
     * @param string $principal The ACL principal.
     * @param string $host The ACL host.
     * @param int $operation The ACL operation.
     * @param int $permissionType The ACL permission type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
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
        return new self($principal, $host, $operation, $permissionType, $unknownTags);
    }
}
