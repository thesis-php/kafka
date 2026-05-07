<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class LeaveGroupResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param list<LeaveGroupResponse\MemberResponse> $members List of leaving member responses.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $members,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $throttleTimeMs = 0;
        if ($version >= 1) {
            $throttleTimeMs = $reader->readInt32();
        }
        $errorCode = $reader->readInt16();
        $members = [];
        if ($version >= 3) {
            if ($flexible) {
                $membersCount = $reader->readCompactArrayLength();
            } else {
                $membersCount = $reader->readArrayLength();
            }
            $members = [];
            for ($membersIndex = 0; $membersIndex < $membersCount; ++$membersIndex) {
                $item = LeaveGroupResponse\MemberResponse::decode($reader, $version);
                $members[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $members, $unknownTags);
    }
}
