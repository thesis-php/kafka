<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateTopicsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatableTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param int $numPartitions The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
     * @param int $replicationFactor The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
     * @param list<CreatableTopic\CreatableReplicaAssignment> $assignments The manual partition assignment, or the empty array if we are using automatic assignment.
     * @param list<CreatableTopic\CreatableTopicConfig> $configs The custom topic configurations to set.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $numPartitions,
        public int $replicationFactor,
        public array $assignments,
        public array $configs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        $writer->writeInt32($this->numPartitions);
        $writer->writeInt16($this->replicationFactor);
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->assignments));
        } else {
            $writer->writeArrayLength(\count($this->assignments));
        }
        foreach ($this->assignments as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->configs));
        } else {
            $writer->writeArrayLength(\count($this->configs));
        }
        foreach ($this->configs as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $numPartitions = $reader->readInt32();
        $replicationFactor = $reader->readInt16();
        if ($flexible) {
            $assignmentsCount = $reader->readCompactArrayLength();
        } else {
            $assignmentsCount = $reader->readArrayLength();
        }
        $assignments = [];
        for ($assignmentsIndex = 0; $assignmentsIndex < $assignmentsCount; ++$assignmentsIndex) {
            $item = CreatableTopic\CreatableReplicaAssignment::decode($reader, $version);
            $assignments[] = $item;
        }
        if ($flexible) {
            $configsCount = $reader->readCompactArrayLength();
        } else {
            $configsCount = $reader->readArrayLength();
        }
        $configs = [];
        for ($configsIndex = 0; $configsIndex < $configsCount; ++$configsIndex) {
            $item = CreatableTopic\CreatableTopicConfig::decode($reader, $version);
            $configs[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $numPartitions, $replicationFactor, $assignments, $configs, $unknownTags);
    }
}
