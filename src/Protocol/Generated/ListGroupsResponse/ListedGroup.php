<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListGroupsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ListedGroup implements Encodable, Decodable
{
    /**
     * @param string $groupId The group ID.
     * @param string $protocolType The group protocol type.
     * @param string $groupState The group state name.
     * @param string $groupType The group type name.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public string $protocolType,
        public string $groupState,
        public string $groupType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        if ($flexible) {
            $writer->writeCompactString($this->protocolType);
        } else {
            $writer->writeString($this->protocolType);
        }
        if ($version >= 4) {
            $writer->writeCompactString($this->groupState);
        }
        if ($version >= 5) {
            $writer->writeCompactString($this->groupType);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        if ($flexible) {
            $groupId = $reader->readCompactString();
        } else {
            $groupId = $reader->readString();
        }
        if ($flexible) {
            $protocolType = $reader->readCompactString();
        } else {
            $protocolType = $reader->readString();
        }
        $groupState = '';
        if ($version >= 4) {
            $groupState = $reader->readCompactString();
        }
        $groupType = '';
        if ($version >= 5) {
            $groupType = $reader->readCompactString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($groupId, $protocolType, $groupState, $groupType, $unknownTags);
    }
}
