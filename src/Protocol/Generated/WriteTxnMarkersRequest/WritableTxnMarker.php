<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\WriteTxnMarkersRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class WritableTxnMarker implements Encodable, Decodable
{
    /**
     * @param int $producerId The current producer ID.
     * @param int $producerEpoch The current epoch associated with the producer ID.
     * @param bool $transactionResult The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
     * @param list<WritableTxnMarker\WritableTxnMarkerTopic> $topics Each topic that we want to write transaction marker(s) for.
     * @param int $coordinatorEpoch Epoch associated with the transaction state partition hosted by this transaction coordinator.
     * @param int $transactionVersion Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $producerId,
        public int $producerEpoch,
        public bool $transactionResult,
        public array $topics,
        public int $coordinatorEpoch,
        public int $transactionVersion,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt64($this->producerId);
        $writer->writeInt16($this->producerEpoch);
        $writer->writeBool($this->transactionResult);
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->coordinatorEpoch);
        if ($version >= 2) {
            $writer->writeInt8($this->transactionVersion);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt16();
        $transactionResult = $reader->readBool();
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = WritableTxnMarker\WritableTxnMarkerTopic::decode($reader, $version);
            $topics[] = $item;
        }
        $coordinatorEpoch = $reader->readInt32();
        $transactionVersion = 0;
        if ($version >= 2) {
            $transactionVersion = $reader->readInt8();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($producerId, $producerEpoch, $transactionResult, $topics, $coordinatorEpoch, $transactionVersion, $unknownTags);
    }
}
