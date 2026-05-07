<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeTopicPartitionsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Cursor implements Encodable, Decodable
{
    /**
     * @param string $topicName The name for the first topic to process.
     * @param int $partitionIndex The partition index to start with.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicName,
        public int $partitionIndex,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->topicName);
        $writer->writeInt32($this->partitionIndex);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicName = $reader->readCompactString();
        $partitionIndex = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($topicName, $partitionIndex, $unknownTags);
    }
}
