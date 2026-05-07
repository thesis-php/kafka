<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareAcknowledgeRequest\AcknowledgeTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AcknowledgePartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param list<AcknowledgePartition\AcknowledgementBatch> $acknowledgementBatches Record batches to acknowledge.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public array $acknowledgementBatches,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
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
        $acknowledgementBatchesCount = $reader->readCompactArrayLength();
        $acknowledgementBatches = [];
        for ($acknowledgementBatchesIndex = 0; $acknowledgementBatchesIndex < $acknowledgementBatchesCount; ++$acknowledgementBatchesIndex) {
            $item = AcknowledgePartition\AcknowledgementBatch::decode($reader, $version);
            $acknowledgementBatches[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $acknowledgementBatches, $unknownTags);
    }
}
