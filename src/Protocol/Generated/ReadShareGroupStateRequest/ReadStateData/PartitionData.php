<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ReadShareGroupStateRequest\ReadStateData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionData implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $leaderEpoch The leader epoch of the share-partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $leaderEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $leaderEpoch, $unknownTags);
    }
}
