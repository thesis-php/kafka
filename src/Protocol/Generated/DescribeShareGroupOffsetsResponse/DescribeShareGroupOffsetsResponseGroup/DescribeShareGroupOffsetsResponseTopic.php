<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeShareGroupOffsetsResponse\DescribeShareGroupOffsetsResponseGroup;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsResponseTopic implements Encodable, Decodable
{
    /**
     * @param string $topicName The topic name.
     * @param UuidInterface $topicId The unique topic ID.
     * @param list<DescribeShareGroupOffsetsResponseTopic\DescribeShareGroupOffsetsResponsePartition> $partitions
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicName,
        public UuidInterface $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->topicName);
        $writer->writeUuid($this->topicId);
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicName = $reader->readCompactString();
        $topicId = $reader->readUuid();
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = DescribeShareGroupOffsetsResponseTopic\DescribeShareGroupOffsetsResponsePartition::decode($reader, $version);
            $partitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($topicName, $topicId, $partitions, $unknownTags);
    }
}
