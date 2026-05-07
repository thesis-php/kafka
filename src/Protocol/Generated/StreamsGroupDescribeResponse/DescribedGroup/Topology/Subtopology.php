<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\DescribedGroup\Topology;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\TopicInfo;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Subtopology implements Encodable, Decodable
{
    /**
     * @param string $subtopologyId String to uniquely identify the subtopology.
     * @param list<string> $sourceTopics The topics the subtopology reads from.
     * @param list<string> $repartitionSinkTopics The repartition topics the subtopology writes to.
     * @param list<TopicInfo> $stateChangelogTopics The set of state changelog topics associated with this subtopology. Created automatically.
     * @param list<TopicInfo> $repartitionSourceTopics The set of source topics that are internally created repartition topics. Created automatically.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $subtopologyId,
        public array $sourceTopics,
        public array $repartitionSinkTopics,
        public array $stateChangelogTopics,
        public array $repartitionSourceTopics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->subtopologyId);
        $writer->writeCompactArrayLength(\count($this->sourceTopics));
        foreach ($this->sourceTopics as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactArrayLength(\count($this->repartitionSinkTopics));
        foreach ($this->repartitionSinkTopics as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactArrayLength(\count($this->stateChangelogTopics));
        foreach ($this->stateChangelogTopics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->repartitionSourceTopics));
        foreach ($this->repartitionSourceTopics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $subtopologyId = $reader->readCompactString();
        $sourceTopicsCount = $reader->readCompactArrayLength();
        $sourceTopics = [];
        for ($sourceTopicsIndex = 0; $sourceTopicsIndex < $sourceTopicsCount; ++$sourceTopicsIndex) {
            $item = $reader->readCompactString();
            $sourceTopics[] = $item;
        }
        $repartitionSinkTopicsCount = $reader->readCompactArrayLength();
        $repartitionSinkTopics = [];
        for ($repartitionSinkTopicsIndex = 0; $repartitionSinkTopicsIndex < $repartitionSinkTopicsCount; ++$repartitionSinkTopicsIndex) {
            $item = $reader->readCompactString();
            $repartitionSinkTopics[] = $item;
        }
        $stateChangelogTopicsCount = $reader->readCompactArrayLength();
        $stateChangelogTopics = [];
        for ($stateChangelogTopicsIndex = 0; $stateChangelogTopicsIndex < $stateChangelogTopicsCount; ++$stateChangelogTopicsIndex) {
            $item = TopicInfo::decode($reader, $version);
            $stateChangelogTopics[] = $item;
        }
        $repartitionSourceTopicsCount = $reader->readCompactArrayLength();
        $repartitionSourceTopics = [];
        for ($repartitionSourceTopicsIndex = 0; $repartitionSourceTopicsIndex < $repartitionSourceTopicsCount; ++$repartitionSourceTopicsIndex) {
            $item = TopicInfo::decode($reader, $version);
            $repartitionSourceTopics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($subtopologyId, $sourceTopics, $repartitionSinkTopics, $stateChangelogTopics, $repartitionSourceTopics, $unknownTags);
    }
}
