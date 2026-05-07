<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareFetchResponse\ShareFetchableTopicResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionData implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $errorCode The fetch error code, or 0 if there was no fetch error.
     * @param ?string $errorMessage The fetch error message, or null if there was no fetch error.
     * @param int $acknowledgeErrorCode The acknowledge error code, or 0 if there was no acknowledge error.
     * @param ?string $acknowledgeErrorMessage The acknowledge error message, or null if there was no acknowledge error.
     * @param PartitionData\LeaderIdAndEpoch $currentLeader The current leader of the partition.
     * @param ?string $records The record data.
     * @param list<PartitionData\AcquiredRecords> $acquiredRecords The acquired records.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public ?string $errorMessage,
        public int $acknowledgeErrorCode,
        public ?string $acknowledgeErrorMessage,
        public PartitionData\LeaderIdAndEpoch $currentLeader,
        public ?string $records,
        public array $acquiredRecords,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeInt16($this->acknowledgeErrorCode);
        $writer->writeCompactNullableString($this->acknowledgeErrorMessage);
        $this->currentLeader->encode($writer, $version);
        if ($version <= 0) {
            $writer->writeCompactNullableBytes($this->records);
        } else {
            if ($this->records === null) {
                throw new \UnexpectedValueException("Field 'Records' must not be null in version '{$version}'");
            }
            $writer->writeCompactBytes($this->records);
        }
        $writer->writeCompactArrayLength(\count($this->acquiredRecords));
        foreach ($this->acquiredRecords as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $acknowledgeErrorCode = $reader->readInt16();
        $acknowledgeErrorMessage = $reader->readCompactNullableString();
        $currentLeader = PartitionData\LeaderIdAndEpoch::decode($reader, $version);
        if ($version <= 0) {
            $records = $reader->readCompactNullableBytes();
        } else {
            $records = $reader->readCompactBytes();
        }
        $acquiredRecordsCount = $reader->readCompactArrayLength();
        $acquiredRecords = [];
        for ($acquiredRecordsIndex = 0; $acquiredRecordsIndex < $acquiredRecordsCount; ++$acquiredRecordsIndex) {
            $item = PartitionData\AcquiredRecords::decode($reader, $version);
            $acquiredRecords[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $errorMessage, $acknowledgeErrorCode, $acknowledgeErrorMessage, $currentLeader, $records, $acquiredRecords, $unknownTags);
    }
}
