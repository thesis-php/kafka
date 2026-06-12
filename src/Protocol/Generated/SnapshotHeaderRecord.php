<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class SnapshotHeaderRecord implements Encodable, Decodable
{
    /**
     * @param int $version The version of the snapshot header record.
     * @param int $lastContainedLogTimestamp The append time of the last record from the log contained in this snapshot.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $version,
        public int $lastContainedLogTimestamp,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->version);
        $writer->writeInt64($this->lastContainedLogTimestamp);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $version = $reader->readInt16();
        $lastContainedLogTimestamp = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($version, $lastContainedLogTimestamp, $unknownTags);
    }
}
