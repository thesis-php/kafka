<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\MetadataResponse\MetadataResponseTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class MetadataResponsePartition implements Encodable, Decodable
{
    /**
     * @param int $errorCode The partition error, or 0 if there was no error.
     * @param int $partitionIndex The partition index.
     * @param int $leaderId The ID of the leader broker.
     * @param int $leaderEpoch The leader epoch of this partition.
     * @param list<int> $replicaNodes The set of all nodes that host this partition.
     * @param list<int> $isrNodes The set of nodes that are in sync with the leader for this partition.
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
        public array $offlineReplicas,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        $writer->writeInt16($this->errorCode);
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt32($this->leaderId);
        if ($version >= 7) {
            $writer->writeInt32($this->leaderEpoch);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->replicaNodes));
        } else {
            $writer->writeArrayLength(\count($this->replicaNodes));
        }
        foreach ($this->replicaNodes as $item) {
            $writer->writeInt32($item);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->isrNodes));
        } else {
            $writer->writeArrayLength(\count($this->isrNodes));
        }
        foreach ($this->isrNodes as $item) {
            $writer->writeInt32($item);
        }
        if ($version >= 5) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->offlineReplicas));
            } else {
                $writer->writeArrayLength(\count($this->offlineReplicas));
            }
            foreach ($this->offlineReplicas as $item) {
                $writer->writeInt32($item);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $errorCode = $reader->readInt16();
        $partitionIndex = $reader->readInt32();
        $leaderId = $reader->readInt32();
        $leaderEpoch = -1;
        if ($version >= 7) {
            $leaderEpoch = $reader->readInt32();
        }
        if ($flexible) {
            $replicaNodesCount = $reader->readCompactArrayLength();
        } else {
            $replicaNodesCount = $reader->readArrayLength();
        }
        $replicaNodes = [];
        for ($replicaNodesIndex = 0; $replicaNodesIndex < $replicaNodesCount; ++$replicaNodesIndex) {
            $item = $reader->readInt32();
            $replicaNodes[] = $item;
        }
        if ($flexible) {
            $isrNodesCount = $reader->readCompactArrayLength();
        } else {
            $isrNodesCount = $reader->readArrayLength();
        }
        $isrNodes = [];
        for ($isrNodesIndex = 0; $isrNodesIndex < $isrNodesCount; ++$isrNodesIndex) {
            $item = $reader->readInt32();
            $isrNodes[] = $item;
        }
        $offlineReplicas = [];
        if ($version >= 5) {
            if ($flexible) {
                $offlineReplicasCount = $reader->readCompactArrayLength();
            } else {
                $offlineReplicasCount = $reader->readArrayLength();
            }
            $offlineReplicas = [];
            for ($offlineReplicasIndex = 0; $offlineReplicasIndex < $offlineReplicasCount; ++$offlineReplicasIndex) {
                $item = $reader->readInt32();
                $offlineReplicas[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $partitionIndex, $leaderId, $leaderEpoch, $replicaNodes, $isrNodes, $offlineReplicas, $unknownTags);
    }
}
