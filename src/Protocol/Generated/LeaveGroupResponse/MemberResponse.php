<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\LeaveGroupResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class MemberResponse implements Encodable, Decodable
{
    /**
     * @param string $memberId The member ID to remove from the group.
     * @param ?string $groupInstanceId The group instance ID to remove from the group.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public ?string $groupInstanceId,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($flexible) {
            $writer->writeCompactNullableString($this->groupInstanceId);
        } else {
            $writer->writeNullableString($this->groupInstanceId);
        }
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $memberId = $reader->readCompactString();
        } else {
            $memberId = $reader->readString();
        }
        if ($flexible) {
            $groupInstanceId = $reader->readCompactNullableString();
        } else {
            $groupInstanceId = $reader->readNullableString();
        }
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($memberId, $groupInstanceId, $errorCode, $unknownTags);
    }
}
