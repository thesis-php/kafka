<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ProduceResponse;

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
final readonly class TopicProduceResponse implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param UuidInterface $topicId The unique topic ID
     * @param list<TopicProduceResponse\PartitionProduceResponse> $partitionResponses Each partition that we produced to within the topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public UuidInterface $topicId,
        public array $partitionResponses,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        if ($version <= 12) {
            if ($flexible) {
                $writer->writeCompactString($this->name);
            } else {
                $writer->writeString($this->name);
            }
        }
        if ($version >= 13) {
            $writer->writeUuid($this->topicId);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitionResponses));
        } else {
            $writer->writeArrayLength(\count($this->partitionResponses));
        }
        foreach ($this->partitionResponses as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $name = '';
        if ($version <= 12) {
            if ($flexible) {
                $name = $reader->readCompactString();
            } else {
                $name = $reader->readString();
            }
        }
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 13) {
            $topicId = $reader->readUuid();
        }
        if ($flexible) {
            $partitionResponsesCount = $reader->readCompactArrayLength();
        } else {
            $partitionResponsesCount = $reader->readArrayLength();
        }
        $partitionResponses = [];
        for ($partitionResponsesIndex = 0; $partitionResponsesIndex < $partitionResponsesCount; ++$partitionResponsesIndex) {
            $item = TopicProduceResponse\PartitionProduceResponse::decode($reader, $version);
            $partitionResponses[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $partitionResponses, $unknownTags);
    }
}
