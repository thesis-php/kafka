<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TaskOffset implements Encodable, Decodable
{
    /**
     * @param string $subtopologyId The subtopology identifier.
     * @param int $partition The partition.
     * @param int $offset The offset.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $subtopologyId,
        public int $partition,
        public int $offset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->subtopologyId);
        $writer->writeInt32($this->partition);
        $writer->writeInt64($this->offset);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $subtopologyId = $reader->readCompactString();
        $partition = $reader->readInt32();
        $offset = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($subtopologyId, $partition, $offset, $unknownTags);
    }
}
