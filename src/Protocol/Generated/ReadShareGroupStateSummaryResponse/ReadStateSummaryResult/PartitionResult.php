<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ReadShareGroupStateSummaryResponse\ReadStateSummaryResult;

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
     * @param int $leaderEpoch The leader epoch of the share-partition.
     * @param int $startOffset The share-partition start offset.
     * @param int $deliveryCompleteCount The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $errorCode,
        public ?string $errorMessage,
        public int $stateEpoch,
        public int $leaderEpoch,
        public int $startOffset,
        public int $deliveryCompleteCount,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeInt32($this->stateEpoch);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeInt64($this->startOffset);
        if ($version >= 1) {
            $writer->writeInt32($this->deliveryCompleteCount);
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
        $leaderEpoch = $reader->readInt32();
        $startOffset = $reader->readInt64();
        $deliveryCompleteCount = -1;
        if ($version >= 1) {
            $deliveryCompleteCount = $reader->readInt32();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $errorCode, $errorMessage, $stateEpoch, $leaderEpoch, $startOffset, $deliveryCompleteCount, $unknownTags);
    }
}
