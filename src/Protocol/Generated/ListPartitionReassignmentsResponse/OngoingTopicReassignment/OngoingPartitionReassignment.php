<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListPartitionReassignmentsResponse\OngoingTopicReassignment;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OngoingPartitionReassignment implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The index of the partition.
     * @param list<int> $replicas The current replica set.
     * @param list<int> $addingReplicas The set of replicas we are currently adding.
     * @param list<int> $removingReplicas The set of replicas we are currently removing.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public array $replicas,
        public array $addingReplicas,
        public array $removingReplicas,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeCompactArrayLength(\count($this->replicas));
        foreach ($this->replicas as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeCompactArrayLength(\count($this->addingReplicas));
        foreach ($this->addingReplicas as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeCompactArrayLength(\count($this->removingReplicas));
        foreach ($this->removingReplicas as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $replicasCount = $reader->readCompactArrayLength();
        $replicas = [];
        for ($replicasIndex = 0; $replicasIndex < $replicasCount; ++$replicasIndex) {
            $item = $reader->readInt32();
            $replicas[] = $item;
        }
        $addingReplicasCount = $reader->readCompactArrayLength();
        $addingReplicas = [];
        for ($addingReplicasIndex = 0; $addingReplicasIndex < $addingReplicasCount; ++$addingReplicasIndex) {
            $item = $reader->readInt32();
            $addingReplicas[] = $item;
        }
        $removingReplicasCount = $reader->readCompactArrayLength();
        $removingReplicas = [];
        for ($removingReplicasIndex = 0; $removingReplicasIndex < $removingReplicasCount; ++$removingReplicasIndex) {
            $item = $reader->readInt32();
            $removingReplicas[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $replicas, $addingReplicas, $removingReplicas, $unknownTags);
    }
}
