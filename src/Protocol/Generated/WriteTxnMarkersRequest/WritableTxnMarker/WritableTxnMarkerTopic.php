<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\WriteTxnMarkersRequest\WritableTxnMarker;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class WritableTxnMarkerTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param list<int> $partitionIndexes The indexes of the partitions to write transaction markers for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public array $partitionIndexes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->partitionIndexes));
        } else {
            $writer->writeArrayLength(\count($this->partitionIndexes));
        }
        foreach ($this->partitionIndexes as $item) {
            $writer->writeInt32($item);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        if ($flexible) {
            $partitionIndexesCount = $reader->readCompactArrayLength();
        } else {
            $partitionIndexesCount = $reader->readArrayLength();
        }
        $partitionIndexes = [];
        for ($partitionIndexesIndex = 0; $partitionIndexesIndex < $partitionIndexesCount; ++$partitionIndexesIndex) {
            $item = $reader->readInt32();
            $partitionIndexes[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $partitionIndexes, $unknownTags);
    }
}
