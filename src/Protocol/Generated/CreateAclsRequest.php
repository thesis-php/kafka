<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<CreateAclsResponse>
 */
final readonly class CreateAclsRequest implements Request
{
    /**
     * @param list<CreateAclsRequest\AclCreation> $creations The ACLs that we want to create.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $creations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::CreateAcls;
    }

    #[\Override]
    public function responseType(): string
    {
        return CreateAclsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->creations));
        } else {
            $writer->writeArrayLength(\count($this->creations));
        }
        foreach ($this->creations as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
