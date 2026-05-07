<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class FetchSnapshotResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param list<FetchSnapshotResponse\TopicSnapshot> $topics The topics to fetch.
     * @param list<FetchSnapshotResponse\NodeEndpoint> $nodeEndpoints Endpoints for all current-leaders enumerated in PartitionSnapshot.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $topics,
        public array $nodeEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = FetchSnapshotResponse\TopicSnapshot::decode($reader, $version);
            $topics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        if ($version >= 1) {
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $nodeEndpointsCount = $tagReader->readCompactArrayLength();
                $nodeEndpoints = [];
                for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
                    $item = FetchSnapshotResponse\NodeEndpoint::decode($tagReader, $version);
                    $nodeEndpoints[] = $item;
                }
                unset($unknownTags[0]);
            } else {
                $nodeEndpoints = [];
            }
        } else {
            $nodeEndpoints = [];
        }
        return new self($throttleTimeMs, $errorCode, $topics, $nodeEndpoints, $unknownTags);
    }
}
