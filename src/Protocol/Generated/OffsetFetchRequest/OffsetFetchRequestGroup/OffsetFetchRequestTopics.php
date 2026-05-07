<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetFetchRequest\OffsetFetchRequestGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchRequestTopics implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param string $topicId The topic ID.
     * @param list<int> $partitionIndexes The partition indexes we would like to fetch offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $topicId,
        public array $partitionIndexes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($version <= 9) {
            $writer->writeCompactString($this->name);
        }
        if ($version >= 10) {
            $writer->writeUuid($this->topicId);
        }
        $writer->writeCompactArrayLength(\count($this->partitionIndexes));
        foreach ($this->partitionIndexes as $item) {
            $writer->writeInt32($item);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $name = '';
        if ($version <= 9) {
            $name = $reader->readCompactString();
        }
        $topicId = '';
        if ($version >= 10) {
            $topicId = $reader->readUuid();
        }
        $partitionIndexesCount = $reader->readCompactArrayLength();
        $partitionIndexes = [];
        for ($partitionIndexesIndex = 0; $partitionIndexesIndex < $partitionIndexesCount; ++$partitionIndexesIndex) {
            $item = $reader->readInt32();
            $partitionIndexes[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $partitionIndexes, $unknownTags);
    }
}
