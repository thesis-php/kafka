<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ConsumerGroupDescribeResponse;

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
     * @param ?string $errorMessage The top-level error message, or null if there was no error.
     * @param string $groupId The group ID string.
     * @param string $groupState The group state string, or the empty string.
     * @param int $groupEpoch The group epoch.
     * @param int $assignmentEpoch The assignment epoch.
     * @param string $assignorName The selected assignor.
     * @param list<DescribedGroup\Member> $members The members.
     * @param int $authorizedOperations 32-bit bitfield to represent authorized operations for this group.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public string $groupId,
        public string $groupState,
        public int $groupEpoch,
        public int $assignmentEpoch,
        public string $assignorName,
        public array $members,
        public int $authorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeCompactString($this->groupId);
        $writer->writeCompactString($this->groupState);
        $writer->writeInt32($this->groupEpoch);
        $writer->writeInt32($this->assignmentEpoch);
        $writer->writeCompactString($this->assignorName);
        $writer->writeCompactArrayLength(\count($this->members));
        foreach ($this->members as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->authorizedOperations);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $groupId = $reader->readCompactString();
        $groupState = $reader->readCompactString();
        $groupEpoch = $reader->readInt32();
        $assignmentEpoch = $reader->readInt32();
        $assignorName = $reader->readCompactString();
        $membersCount = $reader->readCompactArrayLength();
        $members = [];
        for ($membersIndex = 0; $membersIndex < $membersCount; ++$membersIndex) {
            $item = DescribedGroup\Member::decode($reader, $version);
            $members[] = $item;
        }
        $authorizedOperations = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($errorCode, $errorMessage, $groupId, $groupState, $groupEpoch, $assignmentEpoch, $assignorName, $members, $authorizedOperations, $unknownTags);
    }
}
