<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\JoinGroupResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class JoinGroupResponseMember implements Encodable, Decodable
{
    /**
     * @param string $memberId The group member ID.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param string $metadata The group member metadata.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public ?string $groupInstanceId,
        public string $metadata,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($version >= 5) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->groupInstanceId);
            } else {
                $writer->writeNullableString($this->groupInstanceId);
            }
        }
        if ($flexible) {
            $writer->writeCompactBytes($this->metadata);
        } else {
            $writer->writeBytes($this->metadata);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        if ($flexible) {
            $memberId = $reader->readCompactString();
        } else {
            $memberId = $reader->readString();
        }
        $groupInstanceId = null;
        if ($version >= 5) {
            if ($flexible) {
                $groupInstanceId = $reader->readCompactNullableString();
            } else {
                $groupInstanceId = $reader->readNullableString();
            }
        }
        if ($flexible) {
            $metadata = $reader->readCompactBytes();
        } else {
            $metadata = $reader->readBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($memberId, $groupInstanceId, $metadata, $unknownTags);
    }
}
