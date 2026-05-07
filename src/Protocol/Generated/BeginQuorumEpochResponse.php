<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class BeginQuorumEpochResponse implements Response
{
    /**
     * @param int $errorCode The top level error code.
     * @param list<BeginQuorumEpochResponse\TopicData> $topics The topic data.
     * @param list<BeginQuorumEpochResponse\NodeEndpoint> $nodeEndpoints Endpoints for all leaders enumerated in PartitionData.
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
        $flexible = $version >= 1;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = BeginQuorumEpochResponse\TopicData::decode($reader, $version);
            $topics[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $nodeEndpointsCount = $tagReader->readCompactArrayLength();
                $nodeEndpoints = [];
                for ($nodeEndpointsIndex = 0; $nodeEndpointsIndex < $nodeEndpointsCount; ++$nodeEndpointsIndex) {
                    $item = BeginQuorumEpochResponse\NodeEndpoint::decode($tagReader, $version);
                    $nodeEndpoints[] = $item;
                }
                unset($unknownTags[0]);
            } else {
                $nodeEndpoints = [];
            }
        } else {
            $unknownTags = new UnknownTags();
            $nodeEndpoints = [];
        }
        return new self($errorCode, $topics, $nodeEndpoints, $unknownTags);
    }
}
