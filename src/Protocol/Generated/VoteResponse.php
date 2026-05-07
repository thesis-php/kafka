<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class VoteResponse implements Response
{
    /**
     * @param int $errorCode The top level error code.
     * @param list<VoteResponse\TopicData> $topics The results for each topic.
     * @param list<VoteResponse\NodeEndpoint> $nodeEndpoints Endpoints for all current-leaders enumerated in PartitionData.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public array $topics,
        public array $nodeEndpoints,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = VoteResponse\TopicData::decode($reader, $version);
            $topics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        if ($version >= 1) {
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $nodeEndpointsCount = $tagReader->readCompactArrayLength();
                $nodeEndpoints = [];
                for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
                    $item = VoteResponse\NodeEndpoint::decode($tagReader, $version);
                    $nodeEndpoints[] = $item;
                }
                unset($unknownTags[0]);
            } else {
                $nodeEndpoints = [];
            }
        } else {
            $nodeEndpoints = [];
        }
        return new self($errorCode, $topics, $nodeEndpoints, $unknownTags);
    }
}
