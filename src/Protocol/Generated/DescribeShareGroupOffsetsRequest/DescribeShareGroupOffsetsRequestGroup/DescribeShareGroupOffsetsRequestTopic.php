<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeShareGroupOffsetsRequest\DescribeShareGroupOffsetsRequestGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsRequestTopic implements Encodable, Decodable
{
    /**
     * @param string $topicName The topic name.
     * @param list<int> $partitions The partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicName,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->topicName);
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicName = $reader->readCompactString();
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = $reader->readInt32();
            $partitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($topicName, $partitions, $unknownTags);
    }
}
