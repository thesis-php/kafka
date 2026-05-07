<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ListConfigResourcesResponse>
 */
final readonly class ListConfigResourcesRequest implements Request
{
    /**
     * @param list<int> $resourceTypes The list of resource type. If the list is empty, it uses default supported config resource types.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $resourceTypes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ListConfigResources;
    }

    #[\Override]
    public function responseType(): string
    {
        return ListConfigResourcesResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        if ($version >= 1) {
            $writer->writeCompactArrayLength(\count($this->resourceTypes));
            foreach ($this->resourceTypes as $item) {
                $writer->writeInt8($item);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
