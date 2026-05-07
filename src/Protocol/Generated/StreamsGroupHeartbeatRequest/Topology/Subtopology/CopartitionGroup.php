<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatRequest\Topology\Subtopology;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CopartitionGroup implements Encodable, Decodable
{
    /**
     * @param list<int> $sourceTopics The topics the topology reads from. Index into the array on the subtopology level.
     * @param list<int> $sourceTopicRegex Regular expressions identifying topics the subtopology reads from. Index into the array on the subtopology level.
     * @param list<int> $repartitionSourceTopics The set of source topics that are internally created repartition topics. Index into the array on the subtopology level.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $sourceTopics,
        public array $sourceTopicRegex,
        public array $repartitionSourceTopics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->sourceTopics));
        foreach ($this->sourceTopics as $item) {
            $writer->writeInt16($item);
        }
        $writer->writeCompactArrayLength(\count($this->sourceTopicRegex));
        foreach ($this->sourceTopicRegex as $item) {
            $writer->writeInt16($item);
        }
        $writer->writeCompactArrayLength(\count($this->repartitionSourceTopics));
        foreach ($this->repartitionSourceTopics as $item) {
            $writer->writeInt16($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $sourceTopicsCount = $reader->readCompactArrayLength();
        $sourceTopics = [];
        for ($sourceTopicsIndex = 0; $sourceTopicsIndex < $sourceTopicsCount; ++$sourceTopicsIndex) {
            $item = $reader->readInt16();
            $sourceTopics[] = $item;
        }
        $sourceTopicRegexCount = $reader->readCompactArrayLength();
        $sourceTopicRegex = [];
        for ($sourceTopicRegexIndex = 0; $sourceTopicRegexIndex < $sourceTopicRegexCount; ++$sourceTopicRegexIndex) {
            $item = $reader->readInt16();
            $sourceTopicRegex[] = $item;
        }
        $repartitionSourceTopicsCount = $reader->readCompactArrayLength();
        $repartitionSourceTopics = [];
        for ($repartitionSourceTopicsIndex = 0; $repartitionSourceTopicsIndex < $repartitionSourceTopicsCount; ++$repartitionSourceTopicsIndex) {
            $item = $reader->readInt16();
            $repartitionSourceTopics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($sourceTopics, $sourceTopicRegex, $repartitionSourceTopics, $unknownTags);
    }
}
