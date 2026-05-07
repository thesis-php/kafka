<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<DescribeShareGroupOffsetsResponse\DescribeShareGroupOffsetsResponseGroup> $groups The results for each group.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $groups,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $groupsCount = $reader->readCompactArrayLength();
        $groups = [];
        for ($groupsIndex = 0; $groupsIndex < $groupsCount; ++$groupsIndex) {
            $item = DescribeShareGroupOffsetsResponse\DescribeShareGroupOffsetsResponseGroup::decode($reader, $version);
            $groups[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $groups, $unknownTags);
    }
}
