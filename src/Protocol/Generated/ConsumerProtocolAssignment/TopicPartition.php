<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ConsumerProtocolAssignment;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TopicPartition implements Encodable, Decodable
{
    /**
     * @param string $topic The topic name.
     * @param list<int> $partitions The list of partitions assigned to this consumer.
     */
    public function __construct(
        public string $topic,
        public array $partitions,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeString($this->topic);
        $writer->writeArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $writer->writeInt32($item);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topic = $reader->readString();
        $partitionsCount = $reader->readArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = $reader->readInt32();
            $partitions[] = $item;
        }
        return new self($topic, $partitions);
    }
}
