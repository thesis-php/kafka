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
final readonly class ConsumerProtocolSubscription implements Encodable, Decodable
{
    /**
     * @param list<string> $topics The topics that the member wants to consume.
     * @param ?string $userData User data that will be passed back to the consumer.
     * @param list<ConsumerProtocolSubscription\TopicPartition> $ownedPartitions The partitions that the member owns.
     * @param int $generationId The generation id of the member.
     * @param ?string $rackId The rack id of the member.
     */
    public function __construct(
        public array $topics,
        public ?string $userData,
        public array $ownedPartitions,
        public int $generationId,
        public ?string $rackId,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $writer->writeString($item);
        }
        $writer->writeNullableBytes($this->userData);
        if ($version >= 1) {
            $writer->writeArrayLength(\count($this->ownedPartitions));
            foreach ($this->ownedPartitions as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 2) {
            $writer->writeInt32($this->generationId);
        }
        if ($version >= 3) {
            $writer->writeNullableString($this->rackId);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicsCount = $reader->readArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = $reader->readString();
            $topics[] = $item;
        }
        $userData = $reader->readNullableBytes();
        $ownedPartitions = [];
        if ($version >= 1) {
            $ownedPartitionsCount = $reader->readArrayLength();
            $ownedPartitions = [];
            for ($ownedPartitionsIndex = 0; $ownedPartitionsIndex < $ownedPartitionsCount; ++$ownedPartitionsIndex) {
                $item = ConsumerProtocolSubscription\TopicPartition::decode($reader, $version);
                $ownedPartitions[] = $item;
            }
        }
        $generationId = -1;
        if ($version >= 2) {
            $generationId = $reader->readInt32();
        }
        $rackId = null;
        if ($version >= 3) {
            $rackId = $reader->readNullableString();
        }
        return new self($topics, $userData, $ownedPartitions, $generationId, $rackId);
    }
}
