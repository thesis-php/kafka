<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ProduceRequest;

use Ramsey\Uuid\Uuid;
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
final readonly class TopicProduceData implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param UuidInterface $topicId The unique topic ID
     * @param list<TopicProduceData\PartitionProduceData> $partitionData Each partition to produce to.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public UuidInterface $topicId,
        public array $partitionData,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        if ($version <= 12) {
            if ($flexible) {
                $writer->writeCompactString($this->name);
            } else {
                $writer->writeString($this->name);
            }
        }
        if ($version >= 13) {
            $writer->writeUuid($this->topicId);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitionData));
        } else {
            $writer->writeArrayLength(\count($this->partitionData));
        }
        foreach ($this->partitionData as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $name = '';
        if ($version <= 12) {
            if ($flexible) {
                $name = $reader->readCompactString();
            } else {
                $name = $reader->readString();
            }
        }
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 13) {
            $topicId = $reader->readUuid();
        }
        if ($flexible) {
            $partitionDataCount = $reader->readCompactArrayLength();
        } else {
            $partitionDataCount = $reader->readArrayLength();
        }
        $partitionData = [];
        for ($partitionDataIndex = 0; $partitionDataIndex < $partitionDataCount; ++$partitionDataIndex) {
            $item = TopicProduceData\PartitionProduceData::decode($reader, $version);
            $partitionData[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $partitionData, $unknownTags);
    }
}
