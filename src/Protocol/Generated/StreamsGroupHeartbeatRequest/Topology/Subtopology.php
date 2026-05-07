<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatRequest\Topology;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatRequest\TopicInfo;
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
     * @param string $subtopologyId String to uniquely identify the subtopology. Deterministically generated from the topology
     * @param list<string> $sourceTopics The topics the topology reads from.
     * @param list<string> $sourceTopicRegex The regular expressions identifying topics the subtopology reads from.
     * @param list<TopicInfo> $stateChangelogTopics The set of state changelog topics associated with this subtopology. Created automatically.
     * @param list<string> $repartitionSinkTopics The repartition topics the subtopology writes to.
     * @param list<TopicInfo> $repartitionSourceTopics The set of source topics that are internally created repartition topics. Created automatically.
     * @param list<Subtopology\CopartitionGroup> $copartitionGroups A subset of source topics that must be copartitioned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $subtopologyId,
        public array $sourceTopics,
        public array $sourceTopicRegex,
        public array $stateChangelogTopics,
        public array $repartitionSinkTopics,
        public array $repartitionSourceTopics,
        public array $copartitionGroups,
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
        $writer->writeCompactArrayLength(\count($this->sourceTopicRegex));
        foreach ($this->sourceTopicRegex as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactArrayLength(\count($this->stateChangelogTopics));
        foreach ($this->stateChangelogTopics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->repartitionSinkTopics));
        foreach ($this->repartitionSinkTopics as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactArrayLength(\count($this->repartitionSourceTopics));
        foreach ($this->repartitionSourceTopics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->copartitionGroups));
        foreach ($this->copartitionGroups as $item) {
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
        $sourceTopicRegexCount = $reader->readCompactArrayLength();
        $sourceTopicRegex = [];
        for ($sourceTopicRegexIndex = 0; $sourceTopicRegexIndex < $sourceTopicRegexCount; ++$sourceTopicRegexIndex) {
            $item = $reader->readCompactString();
            $sourceTopicRegex[] = $item;
        }
        $stateChangelogTopicsCount = $reader->readCompactArrayLength();
        $stateChangelogTopics = [];
        for ($stateChangelogTopicsIndex = 0; $stateChangelogTopicsIndex < $stateChangelogTopicsCount; ++$stateChangelogTopicsIndex) {
            $item = TopicInfo::decode($reader, $version);
            $stateChangelogTopics[] = $item;
        }
        $repartitionSinkTopicsCount = $reader->readCompactArrayLength();
        $repartitionSinkTopics = [];
        for ($repartitionSinkTopicsIndex = 0; $repartitionSinkTopicsIndex < $repartitionSinkTopicsCount; ++$repartitionSinkTopicsIndex) {
            $item = $reader->readCompactString();
            $repartitionSinkTopics[] = $item;
        }
        $repartitionSourceTopicsCount = $reader->readCompactArrayLength();
        $repartitionSourceTopics = [];
        for ($repartitionSourceTopicsIndex = 0; $repartitionSourceTopicsIndex < $repartitionSourceTopicsCount; ++$repartitionSourceTopicsIndex) {
            $item = TopicInfo::decode($reader, $version);
            $repartitionSourceTopics[] = $item;
        }
        $copartitionGroupsCount = $reader->readCompactArrayLength();
        $copartitionGroups = [];
        for ($copartitionGroupsIndex = 0; $copartitionGroupsIndex < $copartitionGroupsCount; ++$copartitionGroupsIndex) {
            $item = Subtopology\CopartitionGroup::decode($reader, $version);
            $copartitionGroups[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($subtopologyId, $sourceTopics, $sourceTopicRegex, $stateChangelogTopics, $repartitionSinkTopics, $repartitionSourceTopics, $copartitionGroups, $unknownTags);
    }
}
