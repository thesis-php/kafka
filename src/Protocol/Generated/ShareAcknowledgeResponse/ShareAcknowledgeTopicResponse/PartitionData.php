<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareAcknowledgeResponse\ShareAcknowledgeTopicResponse;

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
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param PartitionData\LeaderIdAndEpoch $currentLeader The current leader of the partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public ?string $errorMessage,
        public PartitionData\LeaderIdAndEpoch $currentLeader,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $this->currentLeader->encode($writer, $version);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $currentLeader = PartitionData\LeaderIdAndEpoch::decode($reader, $version);
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $errorMessage, $currentLeader, $unknownTags);
    }
}
