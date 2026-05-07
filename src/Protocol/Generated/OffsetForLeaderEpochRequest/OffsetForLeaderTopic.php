<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetForLeaderEpochRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetForLeaderTopic implements Encodable, Decodable
{
    /**
     * @param string $topic The topic name.
     * @param list<OffsetForLeaderTopic\OffsetForLeaderPartition> $partitions Each partition to get offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topic,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $writer->writeCompactString($this->topic);
        } else {
            $writer->writeString($this->topic);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitions));
        } else {
            $writer->writeArrayLength(\count($this->partitions));
        }
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $topic = $reader->readCompactString();
        } else {
            $topic = $reader->readString();
        }
        if ($flexible) {
            $partitionsCount = $reader->readCompactArrayLength();
        } else {
            $partitionsCount = $reader->readArrayLength();
        }
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = OffsetForLeaderTopic\OffsetForLeaderPartition::decode($reader, $version);
            $partitions[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($topic, $partitions, $unknownTags);
    }
}
