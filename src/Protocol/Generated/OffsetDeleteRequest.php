<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<OffsetDeleteResponse>
 */
final readonly class OffsetDeleteRequest implements Request
{
    /**
     * @param string $groupId The unique group identifier.
     * @param list<OffsetDeleteRequest\OffsetDeleteRequestTopic> $topics The topics to delete offsets for.
     */
    public function __construct(
        public string $groupId,
        public array $topics,
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::OffsetDelete;
    }

    #[\Override]
    public function responseType(): string
    {
        return OffsetDeleteResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeString($this->groupId);
        $writer->writeArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
    }
}
