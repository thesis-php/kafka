<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<OffsetForLeaderEpochResponse>
 */
final readonly class OffsetForLeaderEpochRequest implements Request
{
    /**
     * @param int $replicaId The broker ID of the follower, of -1 if this request is from a consumer.
     * @param list<OffsetForLeaderEpochRequest\OffsetForLeaderTopic> $topics Each topic to get offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $replicaId,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::OffsetForLeaderEpoch;
    }

    #[\Override]
    public function responseType(): string
    {
        return OffsetForLeaderEpochResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($version >= 3) {
            $writer->writeInt32($this->replicaId);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
