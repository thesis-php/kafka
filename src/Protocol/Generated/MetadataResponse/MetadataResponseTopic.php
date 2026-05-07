<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\MetadataResponse;

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
final readonly class MetadataResponseTopic implements Encodable, Decodable
{
    /**
     * @param int $errorCode The topic error, or 0 if there was no error.
     * @param ?string $name The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
     * @param UuidInterface $topicId The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
     * @param bool $isInternal True if the topic is internal.
     * @param list<MetadataResponseTopic\MetadataResponsePartition> $partitions Each partition in the topic.
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
        $flexible = $version >= 9;
        $writer->writeInt16($this->errorCode);
        if ($version >= 12) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->name);
            } else {
                $writer->writeNullableString($this->name);
            }
        } else {
            if ($this->name === null) {
                throw new \UnexpectedValueException("Field 'Name' must not be null in version '{$version}'");
            }
            if ($flexible) {
                $writer->writeCompactString($this->name);
            } else {
                $writer->writeString($this->name);
            }
        }
        if ($version >= 10) {
            $writer->writeUuid($this->topicId);
        }
        if ($version >= 1) {
            $writer->writeBool($this->isInternal);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitions));
        } else {
            $writer->writeArrayLength(\count($this->partitions));
        }
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 8) {
            $writer->writeInt32($this->topicAuthorizedOperations);
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
        if ($version >= 12) {
            if ($flexible) {
                $name = $reader->readCompactNullableString();
            } else {
                $name = $reader->readNullableString();
            }
        } elseif ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 10) {
            $topicId = $reader->readUuid();
        }
        $isInternal = false;
        if ($version >= 1) {
            $isInternal = $reader->readBool();
        }
        if ($flexible) {
            $partitionsCount = $reader->readCompactArrayLength();
        } else {
            $partitionsCount = $reader->readArrayLength();
        }
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = MetadataResponseTopic\MetadataResponsePartition::decode($reader, $version);
            $partitions[] = $item;
        }
        $topicAuthorizedOperations = -2147483648;
        if ($version >= 8) {
            $topicAuthorizedOperations = $reader->readInt32();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $name, $topicId, $isInternal, $partitions, $topicAuthorizedOperations, $unknownTags);
    }
}
