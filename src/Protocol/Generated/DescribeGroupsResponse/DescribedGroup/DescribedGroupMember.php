<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeGroupsResponse\DescribedGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribedGroupMember implements Encodable, Decodable
{
    /**
     * @param string $memberId The member id.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param string $clientId The client ID used in the member's latest join group request.
     * @param string $clientHost The client host.
     * @param string $memberMetadata The metadata corresponding to the current group protocol in use.
     * @param string $memberAssignment The current assignment provided by the group leader.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public ?string $groupInstanceId,
        public string $clientId,
        public string $clientHost,
        public string $memberMetadata,
        public string $memberAssignment,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($version >= 4) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->groupInstanceId);
            } else {
                $writer->writeNullableString($this->groupInstanceId);
            }
        }
        if ($flexible) {
            $writer->writeCompactString($this->clientId);
        } else {
            $writer->writeString($this->clientId);
        }
        if ($flexible) {
            $writer->writeCompactString($this->clientHost);
        } else {
            $writer->writeString($this->clientHost);
        }
        if ($flexible) {
            $writer->writeCompactBytes($this->memberMetadata);
        } else {
            $writer->writeBytes($this->memberMetadata);
        }
        if ($flexible) {
            $writer->writeCompactBytes($this->memberAssignment);
        } else {
            $writer->writeBytes($this->memberAssignment);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $memberId = $reader->readCompactString();
        } else {
            $memberId = $reader->readString();
        }
        $groupInstanceId = null;
        if ($version >= 4) {
            if ($flexible) {
                $groupInstanceId = $reader->readCompactNullableString();
            } else {
                $groupInstanceId = $reader->readNullableString();
            }
        }
        if ($flexible) {
            $clientId = $reader->readCompactString();
        } else {
            $clientId = $reader->readString();
        }
        if ($flexible) {
            $clientHost = $reader->readCompactString();
        } else {
            $clientHost = $reader->readString();
        }
        if ($flexible) {
            $memberMetadata = $reader->readCompactBytes();
        } else {
            $memberMetadata = $reader->readBytes();
        }
        if ($flexible) {
            $memberAssignment = $reader->readCompactBytes();
        } else {
            $memberAssignment = $reader->readBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($memberId, $groupInstanceId, $clientId, $clientHost, $memberMetadata, $memberAssignment, $unknownTags);
    }
}
