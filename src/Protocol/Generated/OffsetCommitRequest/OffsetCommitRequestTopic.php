<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetCommitRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetCommitRequestTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param string $topicId The topic ID.
     * @param list<OffsetCommitRequestTopic\OffsetCommitRequestPartition> $partitions Each partition to commit offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 8;
        if ($version <= 9) {
            if ($flexible) {
                $writer->writeCompactString($this->name);
            } else {
                $writer->writeString($this->name);
            }
        }
        if ($version >= 10) {
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
        $flexible = $version >= 8;
        $name = '';
        if ($version <= 9) {
            if ($flexible) {
                $name = $reader->readCompactString();
            } else {
                $name = $reader->readString();
            }
        }
        $topicId = '';
        if ($version >= 10) {
            $topicId = $reader->readUuid();
        }
        if ($flexible) {
            $partitionsCount = $reader->readCompactArrayLength();
        } else {
            $partitionsCount = $reader->readArrayLength();
        }
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = OffsetCommitRequestTopic\OffsetCommitRequestPartition::decode($reader, $version);
            $partitions[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $partitions, $unknownTags);
    }
}
