<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareFetchRequest\FetchTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class FetchPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $partitionMaxBytes The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
     * @param list<FetchPartition\AcknowledgementBatch> $acknowledgementBatches Record batches to acknowledge.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $partitionMaxBytes,
        public array $acknowledgementBatches,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        if ($version <= 0) {
            $writer->writeInt32($this->partitionMaxBytes);
        }
        $writer->writeCompactArrayLength(\count($this->acknowledgementBatches));
        foreach ($this->acknowledgementBatches as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $partitionMaxBytes = 0;
        if ($version <= 0) {
            $partitionMaxBytes = $reader->readInt32();
        }
        $acknowledgementBatchesCount = $reader->readCompactArrayLength();
        $acknowledgementBatches = [];
        for ($acknowledgementBatchesIndex = 0; $acknowledgementBatchesIndex < $acknowledgementBatchesCount; ++$acknowledgementBatchesIndex) {
            $item = FetchPartition\AcknowledgementBatch::decode($reader, $version);
            $acknowledgementBatches[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $partitionMaxBytes, $acknowledgementBatches, $unknownTags);
    }
}
