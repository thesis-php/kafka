<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeTopicPartitionsResponse\DescribeTopicPartitionsResponseTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeTopicPartitionsResponsePartition implements Encodable, Decodable
{
    /**
     * @param int $errorCode The partition error, or 0 if there was no error.
     * @param int $partitionIndex The partition index.
     * @param int $leaderId The ID of the leader broker.
     * @param int $leaderEpoch The leader epoch of this partition.
     * @param list<int> $replicaNodes The set of all nodes that host this partition.
     * @param list<int> $isrNodes The set of nodes that are in sync with the leader for this partition.
     * @param ?list<int> $eligibleLeaderReplicas The new eligible leader replicas otherwise.
     * @param ?list<int> $lastKnownElr The last known ELR.
     * @param list<int> $offlineReplicas The set of offline replicas of this partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public int $partitionIndex,
        public int $leaderId,
        public int $leaderEpoch,
        public array $replicaNodes,
        public array $isrNodes,
        public ?array $eligibleLeaderReplicas,
        public ?array $lastKnownElr,
        public array $offlineReplicas,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->errorCode);
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeCompactArrayLength(\count($this->replicaNodes));
        foreach ($this->replicaNodes as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeCompactArrayLength(\count($this->isrNodes));
        foreach ($this->isrNodes as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeCompactNullableArrayLength($this->eligibleLeaderReplicas === null ? null : \count($this->eligibleLeaderReplicas));
        if ($this->eligibleLeaderReplicas !== null) {
            foreach ($this->eligibleLeaderReplicas as $item) {
                $writer->writeInt32($item);
            }
        }
        $writer->writeCompactNullableArrayLength($this->lastKnownElr === null ? null : \count($this->lastKnownElr));
        if ($this->lastKnownElr !== null) {
            foreach ($this->lastKnownElr as $item) {
                $writer->writeInt32($item);
            }
        }
        $writer->writeCompactArrayLength(\count($this->offlineReplicas));
        foreach ($this->offlineReplicas as $item) {
            $writer->writeInt32($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $partitionIndex = $reader->readInt32();
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $replicaNodesCount = $reader->readCompactArrayLength();
        $replicaNodes = [];
        for ($replicaNodesIndex = 0; $replicaNodesIndex < $replicaNodesCount; ++$replicaNodesIndex) {
            $item = $reader->readInt32();
            $replicaNodes[] = $item;
        }
        $isrNodesCount = $reader->readCompactArrayLength();
        $isrNodes = [];
        for ($isrNodesIndex = 0; $isrNodesIndex < $isrNodesCount; ++$isrNodesIndex) {
            $item = $reader->readInt32();
            $isrNodes[] = $item;
        }
        $eligibleLeaderReplicasCount = $reader->readCompactNullableArrayLength();
        if ($eligibleLeaderReplicasCount === null) {
            $eligibleLeaderReplicas = null;
        } else {
            $eligibleLeaderReplicas = [];
            for ($eligibleLeaderReplicasIndex = 0; $eligibleLeaderReplicasIndex < $eligibleLeaderReplicasCount; ++$eligibleLeaderReplicasIndex) {
                $item = $reader->readInt32();
                $eligibleLeaderReplicas[] = $item;
            }
        }
        $lastKnownElrCount = $reader->readCompactNullableArrayLength();
        if ($lastKnownElrCount === null) {
            $lastKnownElr = null;
        } else {
            $lastKnownElr = [];
            for ($lastKnownElrIndex = 0; $lastKnownElrIndex < $lastKnownElrCount; ++$lastKnownElrIndex) {
                $item = $reader->readInt32();
                $lastKnownElr[] = $item;
            }
        }
        $offlineReplicasCount = $reader->readCompactArrayLength();
        $offlineReplicas = [];
        for ($offlineReplicasIndex = 0; $offlineReplicasIndex < $offlineReplicasCount; ++$offlineReplicasIndex) {
            $item = $reader->readInt32();
            $offlineReplicas[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($errorCode, $partitionIndex, $leaderId, $leaderEpoch, $replicaNodes, $isrNodes, $eligibleLeaderReplicas, $lastKnownElr, $offlineReplicas, $unknownTags);
    }
}
