<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class EndpointToPartitions implements Encodable, Decodable
{
    /**
     * @param Endpoint $userEndpoint User-defined endpoint to connect to the node
     * @param list<TopicPartition> $activePartitions All topic partitions materialized by active tasks on the node
     * @param list<TopicPartition> $standbyPartitions All topic partitions materialized by standby tasks on the node
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public Endpoint $userEndpoint,
        public array $activePartitions,
        public array $standbyPartitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $this->userEndpoint->encode($writer, $version);
        $writer->writeCompactArrayLength(\count($this->activePartitions));
        foreach ($this->activePartitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->standbyPartitions));
        foreach ($this->standbyPartitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $userEndpoint = Endpoint::decode($reader, $version);
        $activePartitionsCount = $reader->readCompactArrayLength();
        $activePartitions = [];
        for ($activePartitionsIndex = 0; $activePartitionsIndex < $activePartitionsCount; ++$activePartitionsIndex) {
            $item = TopicPartition::decode($reader, $version);
            $activePartitions[] = $item;
        }
        $standbyPartitionsCount = $reader->readCompactArrayLength();
        $standbyPartitions = [];
        for ($standbyPartitionsIndex = 0; $standbyPartitionsIndex < $standbyPartitionsCount; ++$standbyPartitionsIndex) {
            $item = TopicPartition::decode($reader, $version);
            $standbyPartitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($userEndpoint, $activePartitions, $standbyPartitions, $unknownTags);
    }
}
