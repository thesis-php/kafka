<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetFetchResponse\OffsetFetchResponseGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchResponseTopics implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param string $topicId The topic ID.
     * @param list<OffsetFetchResponseTopics\OffsetFetchResponsePartitions> $partitions The responses per partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($version <= 9) {
            $writer->writeCompactString($this->name);
        }
        if ($version >= 10) {
            $writer->writeUuid($this->topicId);
        }
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $name = '';
        if ($version <= 9) {
            $name = $reader->readCompactString();
        }
        $topicId = '';
        if ($version >= 10) {
            $topicId = $reader->readUuid();
        }
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = OffsetFetchResponseTopics\OffsetFetchResponsePartitions::decode($reader, $version);
            $partitions[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $partitions, $unknownTags);
    }
}
