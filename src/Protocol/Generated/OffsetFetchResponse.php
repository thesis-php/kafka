<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<OffsetFetchResponse\OffsetFetchResponseTopic> $topics The responses per topic.
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param list<OffsetFetchResponse\OffsetFetchResponseGroup> $groups The responses per group id.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $topics,
        public int $errorCode,
        public array $groups,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $throttleTimeMs = 0;
        if ($version >= 3) {
            $throttleTimeMs = $reader->readInt32();
        }
        $topics = [];
        if ($version <= 7) {
            if ($flexible) {
                $topicsCount = $reader->readCompactArrayLength();
            } else {
                $topicsCount = $reader->readArrayLength();
            }
            $topics = [];
            for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
                $item = OffsetFetchResponse\OffsetFetchResponseTopic::decode($reader, $version);
                $topics[] = $item;
            }
        }
        $errorCode = 0;
        if ($version >= 2 && $version <= 7) {
            $errorCode = $reader->readInt16();
        }
        $groups = [];
        if ($version >= 8) {
            $groupsCount = $reader->readCompactArrayLength();
            $groups = [];
            for ($groupsIndex = 0; $groupsIndex < $groupsCount; ++$groupsIndex) {
                $item = OffsetFetchResponse\OffsetFetchResponseGroup::decode($reader, $version);
                $groups[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $topics, $errorCode, $groups, $unknownTags);
    }
}
