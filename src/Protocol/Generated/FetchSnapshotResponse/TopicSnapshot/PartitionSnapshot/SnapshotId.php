<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchSnapshotResponse\TopicSnapshot\PartitionSnapshot;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class SnapshotId implements Encodable, Decodable
{
    /**
     * @param int $endOffset The snapshot end offset.
     * @param int $epoch The snapshot epoch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $endOffset,
        public int $epoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt64($this->endOffset);
        $writer->writeInt32($this->epoch);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $endOffset = $reader->readInt64();
        $epoch = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($endOffset, $epoch, $unknownTags);
    }
}
