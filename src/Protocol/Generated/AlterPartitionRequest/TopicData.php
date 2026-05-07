<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TopicData implements Encodable, Decodable
{
    /**
     * @param string $topicId The ID of the topic to alter ISRs for.
     * @param list<TopicData\PartitionData> $partitions The partitions to alter ISRs for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        if ($version >= 2) {
            $writer->writeUuid($this->topicId);
        }
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicId = '';
        if ($version >= 2) {
            $topicId = $reader->readUuid();
        }
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = TopicData\PartitionData::decode($reader, $version);
            $partitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($topicId, $partitions, $unknownTags);
    }
}
