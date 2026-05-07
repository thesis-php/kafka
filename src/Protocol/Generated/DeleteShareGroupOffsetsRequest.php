<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DeleteShareGroupOffsetsResponse>
 */
final readonly class DeleteShareGroupOffsetsRequest implements Request
{
    /**
     * @param string $groupId The group identifier.
     * @param list<DeleteShareGroupOffsetsRequest\DeleteShareGroupOffsetsRequestTopic> $topics The topics to delete offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DeleteShareGroupOffsets;
    }

    #[\Override]
    public function responseType(): string
    {
        return DeleteShareGroupOffsetsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->groupId);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
