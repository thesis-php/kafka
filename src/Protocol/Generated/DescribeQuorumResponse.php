<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeQuorumResponse implements Response
{
    /**
     * @param int $errorCode The top level error code.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param list<DescribeQuorumResponse\TopicData> $topics The response from the describe quorum API.
     * @param list<DescribeQuorumResponse\Node> $nodes The nodes in the quorum.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public array $topics,
        public array $nodes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $errorMessage = null;
        if ($version >= 2) {
            $errorMessage = $reader->readCompactNullableString();
        }
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = DescribeQuorumResponse\TopicData::decode($reader, $version);
            $topics[] = $item;
        }
        $nodes = [];
        if ($version >= 2) {
            $nodesCount = $reader->readCompactArrayLength();
            $nodes = [];
            for ($nodesIndex = 0; $nodesIndex < $nodesCount; ++$nodesIndex) {
                $item = DescribeQuorumResponse\Node::decode($reader, $version);
                $nodes[] = $item;
            }
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($errorCode, $errorMessage, $topics, $nodes, $unknownTags);
    }
}
