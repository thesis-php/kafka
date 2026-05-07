<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<VoteResponse>
 */
final readonly class VoteRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id.
     * @param int $voterId The replica id of the voter receiving the request.
     * @param list<VoteRequest\TopicData> $topics The topic data.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $voterId,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Vote;
    }

    #[\Override]
    public function responseType(): string
    {
        return VoteResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->clusterId);
        if ($version >= 1) {
            $writer->writeInt32($this->voterId);
        }
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
