<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ConsumerGroupHeartbeatResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Assignment implements Encodable, Decodable
{
    /**
     * @param list<TopicPartitions> $topicPartitions The partitions assigned to the member that can be used immediately.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topicPartitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->topicPartitions));
        foreach ($this->topicPartitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicPartitionsCount = $reader->readCompactArrayLength();
        $topicPartitions = [];
        for ($topicPartitionsIndex = 0; $topicPartitionsIndex < $topicPartitionsCount; ++$topicPartitionsIndex) {
            $item = TopicPartitions::decode($reader, $version);
            $topicPartitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($topicPartitions, $unknownTags);
    }
}
