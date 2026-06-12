<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ConsumerProtocolAssignment implements Encodable, Decodable
{
    /**
     * @param list<ConsumerProtocolAssignment\TopicPartition> $assignedPartitions The list of topics and partitions assigned to this consumer.
     * @param ?string $userData User data.
     */
    public function __construct(
        public array $assignedPartitions,
        public ?string $userData,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeArrayLength(\count($this->assignedPartitions));
        foreach ($this->assignedPartitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeNullableBytes($this->userData);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $assignedPartitionsCount = $reader->readArrayLength();
        $assignedPartitions = [];
        for ($assignedPartitionsIndex = 0; $assignedPartitionsIndex < $assignedPartitionsCount; ++$assignedPartitionsIndex) {
            $item = ConsumerProtocolAssignment\TopicPartition::decode($reader, $version);
            $assignedPartitions[] = $item;
        }
        $userData = $reader->readNullableBytes();
        return new self($assignedPartitions, $userData);
    }
}
