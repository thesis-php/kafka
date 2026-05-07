<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\SyncGroupRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class SyncGroupRequestAssignment implements Encodable, Decodable
{
    /**
     * @param string $memberId The ID of the member to assign.
     * @param string $assignment The member assignment.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public string $assignment,
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
            $writer->writeCompactBytes($this->assignment);
        } else {
            $writer->writeBytes($this->assignment);
        }
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
            $assignment = $reader->readCompactBytes();
        } else {
            $assignment = $reader->readBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($memberId, $assignment, $unknownTags);
    }
}
