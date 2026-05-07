<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ListGroupsResponse>
 */
final readonly class ListGroupsRequest implements Request
{
    /**
     * @param list<string> $statesFilter The states of the groups we want to list. If empty, all groups are returned with their state.
     * @param list<string> $typesFilter The types of the groups we want to list. If empty, all groups are returned with their type.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $statesFilter,
        public array $typesFilter,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ListGroups;
    }

    #[\Override]
    public function responseType(): string
    {
        return ListGroupsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($version >= 4) {
            $writer->writeCompactArrayLength(\count($this->statesFilter));
            foreach ($this->statesFilter as $item) {
                $writer->writeCompactString($item);
            }
        }
        if ($version >= 5) {
            $writer->writeCompactArrayLength(\count($this->typesFilter));
            foreach ($this->typesFilter as $item) {
                $writer->writeCompactString($item);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
