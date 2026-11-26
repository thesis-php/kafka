<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeTopicPartitionsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<DescribeTopicPartitionsResponse\DescribeTopicPartitionsResponseTopic> $topics Each topic in the response.
     * @param ?DescribeTopicPartitionsResponse\Cursor $nextCursor The next topic and partition index to fetch details for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $topics,
        public ?DescribeTopicPartitionsResponse\Cursor $nextCursor,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = DescribeTopicPartitionsResponse\DescribeTopicPartitionsResponseTopic::decode($reader, $version);
            $topics[] = $item;
        }
        $nextCursor = $reader->readInt8() < 0 ? null : DescribeTopicPartitionsResponse\Cursor::decode($reader, $version);
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $topics, $nextCursor, $unknownTags);
    }
}
