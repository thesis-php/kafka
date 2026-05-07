<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchRequest;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class FetchTopic implements Encodable, Decodable
{
    /**
     * @param string $topic The name of the topic to fetch.
     * @param UuidInterface $topicId The unique topic ID.
     * @param list<FetchTopic\FetchPartition> $partitions The partitions to fetch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topic,
        public UuidInterface $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        if ($version <= 12) {
            if ($flexible) {
                $writer->writeCompactString($this->topic);
            } else {
                $writer->writeString($this->topic);
            }
        }
        if ($version >= 13) {
            $writer->writeUuid($this->topicId);
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
        $flexible = $version >= 12;
        $topic = '';
        if ($version <= 12) {
            if ($flexible) {
                $topic = $reader->readCompactString();
            } else {
                $topic = $reader->readString();
            }
        }
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 13) {
            $topicId = $reader->readUuid();
        }
        if ($flexible) {
            $partitionsCount = $reader->readCompactArrayLength();
        } else {
            $partitionsCount = $reader->readArrayLength();
        }
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = FetchTopic\FetchPartition::decode($reader, $version);
            $partitions[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($topic, $topicId, $partitions, $unknownTags);
    }
}
