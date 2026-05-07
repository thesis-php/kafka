<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareGroupDescribeResponse\DescribedGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Generated\ShareGroupDescribeResponse\Assignment;
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
     * @param ?string $rackId The member rack ID.
     * @param int $memberEpoch The current member epoch.
     * @param string $clientId The client ID.
     * @param string $clientHost The client host.
     * @param list<string> $subscribedTopicNames The subscribed topic names.
     * @param Assignment $assignment The current assignment.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $memberId,
        public ?string $rackId,
        public int $memberEpoch,
        public string $clientId,
        public string $clientHost,
        public array $subscribedTopicNames,
        public Assignment $assignment,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->memberId);
        $writer->writeCompactNullableString($this->rackId);
        $writer->writeInt32($this->memberEpoch);
        $writer->writeCompactString($this->clientId);
        $writer->writeCompactString($this->clientHost);
        $writer->writeCompactArrayLength(\count($this->subscribedTopicNames));
        foreach ($this->subscribedTopicNames as $item) {
            $writer->writeCompactString($item);
        }
        $this->assignment->encode($writer, $version);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $memberId = $reader->readCompactString();
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
        $assignment = Assignment::decode($reader, $version);
        $unknownTags = $reader->readTaggedFields();
        return new self($memberId, $rackId, $memberEpoch, $clientId, $clientHost, $subscribedTopicNames, $assignment, $unknownTags);
    }
}
