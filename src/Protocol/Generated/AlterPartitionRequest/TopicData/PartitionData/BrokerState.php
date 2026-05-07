<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionRequest\TopicData\PartitionData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class BrokerState implements Encodable, Decodable
{
    /**
     * @param int $brokerId The ID of the broker.
     * @param int $brokerEpoch The epoch of the broker. It will be -1 if the epoch check is not supported.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public int $brokerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeInt64($this->brokerEpoch);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $brokerId = $reader->readInt32();
        $brokerEpoch = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($brokerId, $brokerEpoch, $unknownTags);
    }
}
