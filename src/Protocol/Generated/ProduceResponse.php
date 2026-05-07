<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ProduceResponse implements Response
{
    /**
     * @param list<ProduceResponse\TopicProduceResponse> $responses Each produce response.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<ProduceResponse\NodeEndpoint> $nodeEndpoints Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $responses,
        public int $throttleTimeMs,
        public array $nodeEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        if ($flexible) {
            $responsesCount = $reader->readCompactArrayLength();
        } else {
            $responsesCount = $reader->readArrayLength();
        }
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = ProduceResponse\TopicProduceResponse::decode($reader, $version);
            $responses[] = $item;
        }
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if ($version >= 10) {
                if (isset($unknownTags[0])) {
                    $tagReader = new Reader($unknownTags[0]);
                    $nodeEndpointsCount = $tagReader->readCompactArrayLength();
                    $nodeEndpoints = [];
                    for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
                        $item = ProduceResponse\NodeEndpoint::decode($tagReader, $version);
                        $nodeEndpoints[] = $item;
                    }
                    unset($unknownTags[0]);
                } else {
                    $nodeEndpoints = [];
                }
            } else {
                $nodeEndpoints = [];
            }
        } else {
            $unknownTags = new UnknownTags();
            $nodeEndpoints = [];
        }
        return new self($responses, $throttleTimeMs, $nodeEndpoints, $unknownTags);
    }
}
