<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeAclsResponse>
 */
final readonly class DescribeAclsRequest implements Request
{
    /**
     * @param int $resourceTypeFilter The resource type.
     * @param ?string $resourceNameFilter The resource name, or null to match any resource name.
     * @param int $patternTypeFilter The resource pattern to match.
     * @param ?string $principalFilter The principal to match, or null to match any principal.
     * @param ?string $hostFilter The host to match, or null to match any host.
     * @param int $operation The operation to match.
     * @param int $permissionType The permission type to match.
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
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeAcls;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeAclsResponse::class;
    }

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
        $writer->writeInt8($this->patternTypeFilter);
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
}
