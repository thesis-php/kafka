<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeTopicPartitionsResponse;

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
final readonly class DescribeTopicPartitionsResponseTopic implements Encodable, Decodable
{
    /**
     * @param int $errorCode The topic error, or 0 if there was no error.
     * @param ?string $name The topic name.
     * @param UuidInterface $topicId The topic id.
     * @param bool $isInternal True if the topic is internal.
     * @param list<DescribeTopicPartitionsResponseTopic\DescribeTopicPartitionsResponsePartition> $partitions Each partition in the topic.
     * @param int $topicAuthorizedOperations 32-bit bitfield to represent authorized operations for this topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $name,
        public UuidInterface $topicId,
        public bool $isInternal,
        public array $partitions,
        public int $topicAuthorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->name);
        $writer->writeUuid($this->topicId);
        $writer->writeBool($this->isInternal);
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->topicAuthorizedOperations);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $name = $reader->readCompactNullableString();
        $topicId = $reader->readUuid();
        $isInternal = $reader->readBool();
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = DescribeTopicPartitionsResponseTopic\DescribeTopicPartitionsResponsePartition::decode($reader, $version);
            $partitions[] = $item;
        }
        $topicAuthorizedOperations = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($errorCode, $name, $topicId, $isInternal, $partitions, $topicAuthorizedOperations, $unknownTags);
    }
}
