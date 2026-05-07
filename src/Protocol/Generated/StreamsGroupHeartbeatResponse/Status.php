<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Status implements Encodable, Decodable
{
    /**
     * @param int $statusCode A code to indicate that a particular status is active for the group membership
     * @param string $statusDetail A string representation of the status.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $statusCode,
        public string $statusDetail,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt8($this->statusCode);
        $writer->writeCompactString($this->statusDetail);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $statusCode = $reader->readInt8();
        $statusDetail = $reader->readCompactString();
        $unknownTags = $reader->readTaggedFields();
        return new self($statusCode, $statusDetail, $unknownTags);
    }
}
