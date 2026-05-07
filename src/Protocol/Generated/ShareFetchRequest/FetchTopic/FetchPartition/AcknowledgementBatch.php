<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ShareFetchRequest\FetchTopic\FetchPartition;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AcknowledgementBatch implements Encodable, Decodable
{
    /**
     * @param int $firstOffset First offset of batch of records to acknowledge.
     * @param int $lastOffset Last offset (inclusive) of batch of records to acknowledge.
     * @param list<int> $acknowledgeTypes Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $firstOffset,
        public int $lastOffset,
        public array $acknowledgeTypes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt64($this->firstOffset);
        $writer->writeInt64($this->lastOffset);
        $writer->writeCompactArrayLength(\count($this->acknowledgeTypes));
        foreach ($this->acknowledgeTypes as $item) {
            $writer->writeInt8($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $firstOffset = $reader->readInt64();
        $lastOffset = $reader->readInt64();
        $acknowledgeTypesCount = $reader->readCompactArrayLength();
        $acknowledgeTypes = [];
        for ($acknowledgeTypesIndex = 0; $acknowledgeTypesIndex < $acknowledgeTypesCount; ++$acknowledgeTypesIndex) {
            $item = $reader->readInt8();
            $acknowledgeTypes[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($firstOffset, $lastOffset, $acknowledgeTypes, $unknownTags);
    }
}
