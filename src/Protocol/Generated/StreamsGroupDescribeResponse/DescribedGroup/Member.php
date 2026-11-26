<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\DescribedGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\Assignment;
use Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\Endpoint;
use Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\KeyValue;
use Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\TaskOffset;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Member implements Encodable, Decodable
{
    /**
     * @param string $memberId The member ID.
     * @param int $memberEpoch The member epoch.
     * @param ?string $instanceId The member instance ID for static membership.
     * @param ?string $rackId The rack ID.
     * @param string $clientId The client ID.
     * @param string $clientHost The client host.
     * @param int $topologyEpoch The epoch of the topology on the client.
     * @param string $processId Identity of the streams instance that may have multiple clients.
     * @param ?Endpoint $userEndpoint User-defined endpoint for Interactive Queries. Null if not defined for this client.
     * @param list<KeyValue> $clientTags Used for rack-aware assignment algorithm.
     * @param list<TaskOffset> $taskOffsets Cumulative changelog offsets for tasks.
     * @param list<TaskOffset> $taskEndOffsets Cumulative changelog end offsets for tasks.
     * @param Assignment $assignment The current assignment.
     * @param Assignment $targetAssignment The target assignment.
     * @param bool $isClassic True for classic members that have not been upgraded yet.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public int $memberEpoch,
        public ?string $instanceId,
        public ?string $rackId,
        public string $clientId,
        public string $clientHost,
        public int $topologyEpoch,
        public string $processId,
        public ?Endpoint $userEndpoint,
        public array $clientTags,
        public array $taskOffsets,
        public array $taskEndOffsets,
        public Assignment $assignment,
        public Assignment $targetAssignment,
        public bool $isClassic,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->memberId);
        $writer->writeInt32($this->memberEpoch);
        $writer->writeCompactNullableString($this->instanceId);
        $writer->writeCompactNullableString($this->rackId);
        $writer->writeCompactString($this->clientId);
        $writer->writeCompactString($this->clientHost);
        $writer->writeInt32($this->topologyEpoch);
        $writer->writeCompactString($this->processId);
        $writer->writeInt8($this->userEndpoint === null ? -1 : 1);
        $this->userEndpoint?->encode($writer, $version);
        $writer->writeCompactArrayLength(\count($this->clientTags));
        foreach ($this->clientTags as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->taskOffsets));
        foreach ($this->taskOffsets as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->taskEndOffsets));
        foreach ($this->taskEndOffsets as $item) {
            $item->encode($writer, $version);
        }
        $this->assignment->encode($writer, $version);
        $this->targetAssignment->encode($writer, $version);
        $writer->writeBool($this->isClassic);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $memberId = $reader->readCompactString();
        $memberEpoch = $reader->readInt32();
        $instanceId = $reader->readCompactNullableString();
        $rackId = $reader->readCompactNullableString();
        $clientId = $reader->readCompactString();
        $clientHost = $reader->readCompactString();
        $topologyEpoch = $reader->readInt32();
        $processId = $reader->readCompactString();
        $userEndpoint = $reader->readInt8() < 0 ? null : Endpoint::decode($reader, $version);
        $clientTagsCount = $reader->readCompactArrayLength();
        $clientTags = [];
        for ($clientTagsIndex = 0; $clientTagsIndex < $clientTagsCount; ++$clientTagsIndex) {
            $item = KeyValue::decode($reader, $version);
            $clientTags[] = $item;
        }
        $taskOffsetsCount = $reader->readCompactArrayLength();
        $taskOffsets = [];
        for ($taskOffsetsIndex = 0; $taskOffsetsIndex < $taskOffsetsCount; ++$taskOffsetsIndex) {
            $item = TaskOffset::decode($reader, $version);
            $taskOffsets[] = $item;
        }
        $taskEndOffsetsCount = $reader->readCompactArrayLength();
        $taskEndOffsets = [];
        for ($taskEndOffsetsIndex = 0; $taskEndOffsetsIndex < $taskEndOffsetsCount; ++$taskEndOffsetsIndex) {
            $item = TaskOffset::decode($reader, $version);
            $taskEndOffsets[] = $item;
        }
        $assignment = Assignment::decode($reader, $version);
        $targetAssignment = Assignment::decode($reader, $version);
        $isClassic = $reader->readBool();
        $unknownTags = $reader->readTaggedFields();
        return new self($memberId, $memberEpoch, $instanceId, $rackId, $clientId, $clientHost, $topologyEpoch, $processId, $userEndpoint, $clientTags, $taskOffsets, $taskEndOffsets, $assignment, $targetAssignment, $isClassic, $unknownTags);
    }
}
