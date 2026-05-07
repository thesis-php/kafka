<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeGroupsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribedGroup implements Encodable, Decodable
{
    /**
     * @param int $errorCode The describe error, or 0 if there was no error.
     * @param ?string $errorMessage The describe error message, or null if there was no error.
     * @param string $groupId The group ID string.
     * @param string $groupState The group state string, or the empty string.
     * @param string $protocolType The group protocol type, or the empty string.
     * @param string $protocolData The group protocol data, or the empty string.
     * @param list<DescribedGroup\DescribedGroupMember> $members The group members.
     * @param int $authorizedOperations 32-bit bitfield to represent authorized operations for this group.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public string $groupId,
        public string $groupState,
        public string $protocolType,
        public string $protocolData,
        public array $members,
        public int $authorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        $writer->writeInt16($this->errorCode);
        if ($version >= 6) {
            $writer->writeCompactNullableString($this->errorMessage);
        }
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        if ($flexible) {
            $writer->writeCompactString($this->groupState);
        } else {
            $writer->writeString($this->groupState);
        }
        if ($flexible) {
            $writer->writeCompactString($this->protocolType);
        } else {
            $writer->writeString($this->protocolType);
        }
        if ($flexible) {
            $writer->writeCompactString($this->protocolData);
        } else {
            $writer->writeString($this->protocolData);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->members));
        } else {
            $writer->writeArrayLength(\count($this->members));
        }
        foreach ($this->members as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 3) {
            $writer->writeInt32($this->authorizedOperations);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        $errorCode = $reader->readInt16();
        $errorMessage = null;
        if ($version >= 6) {
            $errorMessage = $reader->readCompactNullableString();
        }
        if ($flexible) {
            $groupId = $reader->readCompactString();
        } else {
            $groupId = $reader->readString();
        }
        if ($flexible) {
            $groupState = $reader->readCompactString();
        } else {
            $groupState = $reader->readString();
        }
        if ($flexible) {
            $protocolType = $reader->readCompactString();
        } else {
            $protocolType = $reader->readString();
        }
        if ($flexible) {
            $protocolData = $reader->readCompactString();
        } else {
            $protocolData = $reader->readString();
        }
        if ($flexible) {
            $membersCount = $reader->readCompactArrayLength();
        } else {
            $membersCount = $reader->readArrayLength();
        }
        $members = [];
        for ($membersIndex = 0; $membersIndex < $membersCount; ++$membersIndex) {
            $item = DescribedGroup\DescribedGroupMember::decode($reader, $version);
            $members[] = $item;
        }
        $authorizedOperations = -2147483648;
        if ($version >= 3) {
            $authorizedOperations = $reader->readInt32();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $groupId, $groupState, $protocolType, $protocolData, $members, $authorizedOperations, $unknownTags);
    }
}
