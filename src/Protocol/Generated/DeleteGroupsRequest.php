<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DeleteGroupsResponse>
 */
final readonly class DeleteGroupsRequest implements Request
{
    /**
     * @param list<string> $groupsNames The group names to delete.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $groupsNames,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DeleteGroups;
    }

    #[\Override]
    public function responseType(): string
    {
        return DeleteGroupsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->groupsNames));
        } else {
            $writer->writeArrayLength(\count($this->groupsNames));
        }
        foreach ($this->groupsNames as $item) {
            if ($flexible) {
                $writer->writeCompactString($item);
            } else {
                $writer->writeString($item);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
