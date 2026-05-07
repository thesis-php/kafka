<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeProducersResponse\TopicResponse\PartitionResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ProducerState implements Encodable, Decodable
{
    /**
     * @param int $producerId The producer id.
     * @param int $producerEpoch The producer epoch.
     * @param int $lastSequence The last sequence number sent by the producer.
     * @param int $lastTimestamp The last timestamp sent by the producer.
     * @param int $coordinatorEpoch The current epoch of the producer group.
     * @param int $currentTxnStartOffset The current transaction start offset of the producer.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $producerId,
        public int $producerEpoch,
        public int $lastSequence,
        public int $lastTimestamp,
        public int $coordinatorEpoch,
        public int $currentTxnStartOffset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt64($this->producerId);
        $writer->writeInt32($this->producerEpoch);
        $writer->writeInt32($this->lastSequence);
        $writer->writeInt64($this->lastTimestamp);
        $writer->writeInt32($this->coordinatorEpoch);
        $writer->writeInt64($this->currentTxnStartOffset);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt32();
        $lastSequence = $reader->readInt32();
        $lastTimestamp = $reader->readInt64();
        $coordinatorEpoch = $reader->readInt32();
        $currentTxnStartOffset = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($producerId, $producerEpoch, $lastSequence, $lastTimestamp, $coordinatorEpoch, $currentTxnStartOffset, $unknownTags);
    }
}
