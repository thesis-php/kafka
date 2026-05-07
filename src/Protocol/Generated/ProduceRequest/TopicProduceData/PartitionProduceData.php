<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ProduceRequest\TopicProduceData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionProduceData implements Encodable, Decodable
{
    /**
     * @param int $index The partition index.
     * @param ?string $records The record data to be produced.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $index,
        public ?string $records,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        $writer->writeInt32($this->index);
        if ($flexible) {
            $writer->writeCompactNullableBytes($this->records);
        } else {
            $writer->writeNullableBytes($this->records);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $index = $reader->readInt32();
        if ($flexible) {
            $records = $reader->readCompactNullableBytes();
        } else {
            $records = $reader->readNullableBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($index, $records, $unknownTags);
    }
}
