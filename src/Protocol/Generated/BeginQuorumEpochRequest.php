<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<BeginQuorumEpochResponse>
 */
final readonly class BeginQuorumEpochRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id.
     * @param int $voterId The replica id of the voter receiving the request.
     * @param list<BeginQuorumEpochRequest\TopicData> $topics The topics.
     * @param list<BeginQuorumEpochRequest\LeaderEndpoint> $leaderEndpoints Endpoints for the leader.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $voterId,
        public array $topics,
        public array $leaderEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::BeginQuorumEpoch;
    }

    #[\Override]
    public function responseType(): string
    {
        return BeginQuorumEpochResponse::class;
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
        if ($version >= 1) {
            $writer->writeInt32($this->voterId);
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
