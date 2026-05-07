<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListPartitionReassignmentsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ListPartitionReassignmentsTopics implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param list<int> $partitionIndexes The partitions to list partition reassignments for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public array $partitionIndexes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeCompactArrayLength(\count($this->partitionIndexes));
        foreach ($this->partitionIndexes as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $partitionIndexesCount = $reader->readCompactArrayLength();
        $partitionIndexes = [];
        for ($partitionIndexesIndex = 0; $partitionIndexesIndex < $partitionIndexesCount; ++$partitionIndexesIndex) {
            $item = $reader->readInt32();
            $partitionIndexes[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $partitionIndexes, $unknownTags);
    }
}
