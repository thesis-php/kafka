<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class FetchResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param int $sessionId The fetch session ID, or 0 if this is not part of a fetch session.
     * @param list<FetchResponse\FetchableTopicResponse> $responses The response topics.
     * @param list<FetchResponse\NodeEndpoint> $nodeEndpoints Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $sessionId,
        public array $responses,
        public array $nodeEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = 0;
        if ($version >= 7) {
            $errorCode = $reader->readInt16();
        }
        $sessionId = 0;
        if ($version >= 7) {
            $sessionId = $reader->readInt32();
        }
        if ($flexible) {
            $responsesCount = $reader->readCompactArrayLength();
        } else {
            $responsesCount = $reader->readArrayLength();
        }
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = FetchResponse\FetchableTopicResponse::decode($reader, $version);
            $responses[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if ($version >= 16) {
                if (isset($unknownTags[0])) {
                    $tagReader = new Reader($unknownTags[0]);
                    $nodeEndpointsCount = $tagReader->readCompactArrayLength();
                    $nodeEndpoints = [];
                    for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
                        $item = FetchResponse\NodeEndpoint::decode($tagReader, $version);
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
        return new self($throttleTimeMs, $errorCode, $sessionId, $responses, $nodeEndpoints, $unknownTags);
    }
}
