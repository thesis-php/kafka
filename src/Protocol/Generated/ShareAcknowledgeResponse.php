<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ShareAcknowledgeResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param ?string $errorMessage The top-level error message, or null if there was no error.
     * @param int $acquisitionLockTimeoutMs The time in milliseconds for which the acquired records are locked.
     * @param list<ShareAcknowledgeResponse\ShareAcknowledgeTopicResponse> $responses The response topics.
     * @param list<ShareAcknowledgeResponse\NodeEndpoint> $nodeEndpoints Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public int $acquisitionLockTimeoutMs,
        public array $responses,
        public array $nodeEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $acquisitionLockTimeoutMs = 0;
        if ($version >= 2) {
            $acquisitionLockTimeoutMs = $reader->readInt32();
        }
        $responsesCount = $reader->readCompactArrayLength();
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = ShareAcknowledgeResponse\ShareAcknowledgeTopicResponse::decode($reader, $version);
            $responses[] = $item;
        }
        $nodeEndpointsCount = $reader->readCompactArrayLength();
        $nodeEndpoints = [];
        for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
            $item = ShareAcknowledgeResponse\NodeEndpoint::decode($reader, $version);
            $nodeEndpoints[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $errorMessage, $acquisitionLockTimeoutMs, $responses, $nodeEndpoints, $unknownTags);
    }
}
