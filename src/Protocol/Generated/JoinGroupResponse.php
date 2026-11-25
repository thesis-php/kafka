<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class JoinGroupResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $generationId The generation ID of the group.
     * @param ?string $protocolType The group protocol name.
     * @param ?string $protocolName The group protocol selected by the coordinator.
     * @param string $leader The leader of the group.
     * @param bool $skipAssignment True if the leader must skip running the assignment.
     * @param string $memberId The member ID assigned by the group coordinator.
     * @param list<JoinGroupResponse\JoinGroupResponseMember> $members The group members.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $generationId,
        public ?string $protocolType,
        public ?string $protocolName,
        public string $leader,
        public bool $skipAssignment,
        public string $memberId,
        public array $members,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $throttleTimeMs = 0;
        if ($version >= 2) {
            $throttleTimeMs = $reader->readInt32();
        }
        $errorCode = $reader->readInt16();
        $generationId = $reader->readInt32();
        $protocolType = null;
        if ($version >= 7) {
            $protocolType = $reader->readCompactNullableString();
        }
        if ($version >= 7) {
            $protocolName = $reader->readCompactNullableString();
        } elseif ($flexible) {
            $protocolName = $reader->readCompactString();
        } else {
            $protocolName = $reader->readString();
        }
        if ($flexible) {
            $leader = $reader->readCompactString();
        } else {
            $leader = $reader->readString();
        }
        $skipAssignment = false;
        if ($version >= 9) {
            $skipAssignment = $reader->readBool();
        }
        if ($flexible) {
            $memberId = $reader->readCompactString();
        } else {
            $memberId = $reader->readString();
        }
        if ($flexible) {
            $membersCount = $reader->readCompactArrayLength();
        } else {
            $membersCount = $reader->readArrayLength();
        }
        $members = [];
        for ($membersIndex = 0; $membersIndex < $membersCount; ++$membersIndex) {
            $item = JoinGroupResponse\JoinGroupResponseMember::decode($reader, $version);
            $members[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $generationId, $protocolType, $protocolName, $leader, $skipAssignment, $memberId, $members, $unknownTags);
    }
}
