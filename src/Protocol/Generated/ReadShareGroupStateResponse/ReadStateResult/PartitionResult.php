<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ReadShareGroupStateResponse\ReadStateResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionResult implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param int $stateEpoch The state epoch of the share-partition.
     * @param int $startOffset The share-partition start offset, which can be -1 if it is not yet initialized.
     * @param list<PartitionResult\StateBatch> $stateBatches The state batches for this share-partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $errorCode,
        public ?string $errorMessage,
        public int $stateEpoch,
        public int $startOffset,
        public array $stateBatches,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeInt32($this->stateEpoch);
        $writer->writeInt64($this->startOffset);
        $writer->writeCompactArrayLength(\count($this->stateBatches));
        foreach ($this->stateBatches as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $stateEpoch = $reader->readInt32();
        $startOffset = $reader->readInt64();
        $stateBatchesCount = $reader->readCompactArrayLength();
        $stateBatches = [];
        for ($stateBatchesIndex = 0; $stateBatchesIndex < $stateBatchesCount; ++$stateBatchesIndex) {
            $item = PartitionResult\StateBatch::decode($reader, $version);
            $stateBatches[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $errorCode, $errorMessage, $stateEpoch, $startOffset, $stateBatches, $unknownTags);
    }
}
