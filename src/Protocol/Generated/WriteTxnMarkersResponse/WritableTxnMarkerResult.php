<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\WriteTxnMarkersResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class WritableTxnMarkerResult implements Encodable, Decodable
{
    /**
     * @param int $producerId The current producer ID in use by the transactional ID.
     * @param list<WritableTxnMarkerResult\WritableTxnMarkerTopicResult> $topics The results by topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $producerId,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        $writer->writeInt64($this->producerId);
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
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
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = WritableTxnMarkerResult\WritableTxnMarkerTopicResult::decode($reader, $version);
            $topics[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($producerId, $topics, $unknownTags);
    }
}
