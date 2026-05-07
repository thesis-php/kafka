<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterReplicaLogDirsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AlterReplicaLogDirTopicResult implements Encodable, Decodable
{
    /**
     * @param string $topicName The name of the topic.
     * @param list<AlterReplicaLogDirTopicResult\AlterReplicaLogDirPartitionResult> $partitions The results for each partition.
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
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->topicName);
        } else {
            $writer->writeString($this->topicName);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitions));
        } else {
            $writer->writeArrayLength(\count($this->partitions));
        }
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
        $flexible = $version >= 2;
        if ($flexible) {
            $topicName = $reader->readCompactString();
        } else {
            $topicName = $reader->readString();
        }
        if ($flexible) {
            $partitionsCount = $reader->readCompactArrayLength();
        } else {
            $partitionsCount = $reader->readArrayLength();
        }
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = AlterReplicaLogDirTopicResult\AlterReplicaLogDirPartitionResult::decode($reader, $version);
            $partitions[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($topicName, $partitions, $unknownTags);
    }
}
