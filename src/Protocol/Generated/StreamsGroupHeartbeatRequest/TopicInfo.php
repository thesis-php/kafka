<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TopicInfo implements Encodable, Decodable
{
    /**
     * @param string $name The name of the topic.
     * @param int $partitions The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
     * @param int $replicationFactor The replication factor of the topic. Can be 0 if the default replication factor should be used.
     * @param list<KeyValue> $topicConfigs Topic-level configurations as key-value pairs.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $partitions,
        public int $replicationFactor,
        public array $topicConfigs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeInt32($this->partitions);
        $writer->writeInt16($this->replicationFactor);
        $writer->writeCompactArrayLength(\count($this->topicConfigs));
        foreach ($this->topicConfigs as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $partitions = $reader->readInt32();
        $replicationFactor = $reader->readInt16();
        $topicConfigsCount = $reader->readCompactArrayLength();
        $topicConfigs = [];
        for ($topicConfigsIndex = 0; $topicConfigsIndex < $topicConfigsCount; ++$topicConfigsIndex) {
            $item = KeyValue::decode($reader, $version);
            $topicConfigs[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $partitions, $replicationFactor, $topicConfigs, $unknownTags);
    }
}
