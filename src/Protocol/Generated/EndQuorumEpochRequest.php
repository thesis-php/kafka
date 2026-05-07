<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<EndQuorumEpochResponse>
 */
final readonly class EndQuorumEpochRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id.
     * @param list<EndQuorumEpochRequest\TopicData> $topics The topics.
     * @param list<EndQuorumEpochRequest\LeaderEndpoint> $leaderEndpoints Endpoints for the leader.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public array $topics,
        public array $leaderEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::EndQuorumEpoch;
    }

    #[\Override]
    public function responseType(): string
    {
        return EndQuorumEpochResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactNullableString($this->clusterId);
        } else {
            $writer->writeNullableString($this->clusterId);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 1) {
            $writer->writeCompactArrayLength(\count($this->leaderEndpoints));
            foreach ($this->leaderEndpoints as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
