<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DeleteAclsResponse>
 */
final readonly class DeleteAclsRequest implements Request
{
    /**
     * @param list<DeleteAclsRequest\DeleteAclsFilter> $filters The filters to use when deleting ACLs.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $filters,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DeleteAcls;
    }

    #[\Override]
    public function responseType(): string
    {
        return DeleteAclsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->filters));
        } else {
            $writer->writeArrayLength(\count($this->filters));
        }
        foreach ($this->filters as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
