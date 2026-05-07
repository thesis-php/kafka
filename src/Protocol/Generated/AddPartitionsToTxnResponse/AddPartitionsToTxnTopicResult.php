<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AddPartitionsToTxnResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AddPartitionsToTxnTopicResult implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param list<AddPartitionsToTxnPartitionResult> $resultsByPartition The results for each partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public array $resultsByPartition,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->resultsByPartition));
        } else {
            $writer->writeArrayLength(\count($this->resultsByPartition));
        }
        foreach ($this->resultsByPartition as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        if ($flexible) {
            $resultsByPartitionCount = $reader->readCompactArrayLength();
        } else {
            $resultsByPartitionCount = $reader->readArrayLength();
        }
        $resultsByPartition = [];
        for ($resultsByPartitionIndex = 0; $resultsByPartitionIndex < $resultsByPartitionCount; ++$resultsByPartitionIndex) {
            $item = AddPartitionsToTxnPartitionResult::decode($reader, $version);
            $resultsByPartition[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $resultsByPartition, $unknownTags);
    }
}
