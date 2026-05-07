<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionReassignmentsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ReassignableTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param list<ReassignableTopic\ReassignablePartition> $partitions The partitions to reassign.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = ReassignableTopic\ReassignablePartition::decode($reader, $version);
            $partitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $partitions, $unknownTags);
    }
}
