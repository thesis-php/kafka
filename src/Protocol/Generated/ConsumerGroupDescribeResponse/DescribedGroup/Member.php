<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ConsumerGroupDescribeResponse\DescribedGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\ConsumerGroupDescribeResponse\Assignment;
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
     * @param ?string $instanceId The member instance ID.
     * @param ?string $rackId The member rack ID.
     * @param int $memberEpoch The current member epoch.
     * @param string $clientId The client ID.
     * @param string $clientHost The client host.
     * @param list<string> $subscribedTopicNames The subscribed topic names.
     * @param ?string $subscribedTopicRegex the subscribed topic regex otherwise or null of not provided.
     * @param Assignment $assignment The current assignment.
     * @param Assignment $targetAssignment The target assignment.
     * @param int $memberType -1 for unknown. 0 for classic member. +1 for consumer member.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public ?string $instanceId,
        public ?string $rackId,
        public int $memberEpoch,
        public string $clientId,
        public string $clientHost,
        public array $subscribedTopicNames,
        public ?string $subscribedTopicRegex,
        public Assignment $assignment,
        public Assignment $targetAssignment,
        public int $memberType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->memberId);
        $writer->writeCompactNullableString($this->instanceId);
        $writer->writeCompactNullableString($this->rackId);
        $writer->writeInt32($this->memberEpoch);
        $writer->writeCompactString($this->clientId);
        $writer->writeCompactString($this->clientHost);
        $writer->writeCompactArrayLength(\count($this->subscribedTopicNames));
        foreach ($this->subscribedTopicNames as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactNullableString($this->subscribedTopicRegex);
        $this->assignment->encode($writer, $version);
        $this->targetAssignment->encode($writer, $version);
        if ($version >= 1) {
            $writer->writeInt8($this->memberType);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $memberId = $reader->readCompactString();
        $instanceId = $reader->readCompactNullableString();
        $rackId = $reader->readCompactNullableString();
        $memberEpoch = $reader->readInt32();
        $clientId = $reader->readCompactString();
        $clientHost = $reader->readCompactString();
        $subscribedTopicNamesCount = $reader->readCompactArrayLength();
        $subscribedTopicNames = [];
        for ($subscribedTopicNamesIndex = 0; $subscribedTopicNamesIndex < $subscribedTopicNamesCount; ++$subscribedTopicNamesIndex) {
            $item = $reader->readCompactString();
            $subscribedTopicNames[] = $item;
        }
        $subscribedTopicRegex = $reader->readCompactNullableString();
        $assignment = Assignment::decode($reader, $version);
        $targetAssignment = Assignment::decode($reader, $version);
        $memberType = -1;
        if ($version >= 1) {
            $memberType = $reader->readInt8();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($memberId, $instanceId, $rackId, $memberEpoch, $clientId, $clientHost, $subscribedTopicNames, $subscribedTopicRegex, $assignment, $targetAssignment, $memberType, $unknownTags);
    }
}
