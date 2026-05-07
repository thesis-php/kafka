<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ListGroupsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param list<ListGroupsResponse\ListedGroup> $groups Each group in the response.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $groups,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $throttleTimeMs = 0;
        if ($version >= 1) {
            $throttleTimeMs = $reader->readInt32();
        }
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $groupsCount = $reader->readCompactArrayLength();
        } else {
            $groupsCount = $reader->readArrayLength();
        }
        $groups = [];
        for ($groupsIndex = 0; $groupsIndex < $groupsCount; ++$groupsIndex) {
            $item = ListGroupsResponse\ListedGroup::decode($reader, $version);
            $groups[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $groups, $unknownTags);
    }
}
