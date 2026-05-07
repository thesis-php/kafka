<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ElectLeadersResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ReplicaElectionResult implements Encodable, Decodable
{
    /**
     * @param string $topic The topic name.
     * @param list<ReplicaElectionResult\PartitionResult> $partitionResult The results for each partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topic,
        public array $partitionResult,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->topic);
        } else {
            $writer->writeString($this->topic);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitionResult));
        } else {
            $writer->writeArrayLength(\count($this->partitionResult));
        }
        foreach ($this->partitionResult as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $topic = $reader->readCompactString();
        } else {
            $topic = $reader->readString();
        }
        if ($flexible) {
            $partitionResultCount = $reader->readCompactArrayLength();
        } else {
            $partitionResultCount = $reader->readArrayLength();
        }
        $partitionResult = [];
        for ($partitionResultIndex = 0; $partitionResultIndex < $partitionResultCount; ++$partitionResultIndex) {
            $item = ReplicaElectionResult\PartitionResult::decode($reader, $version);
            $partitionResult[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($topic, $partitionResult, $unknownTags);
    }
}
