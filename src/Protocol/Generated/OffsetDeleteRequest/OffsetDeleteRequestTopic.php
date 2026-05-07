<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetDeleteRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetDeleteRequestTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param list<OffsetDeleteRequestTopic\OffsetDeleteRequestPartition> $partitions Each partition to delete offsets for.
     */
    public function __construct(
        public string $name,
        public array $partitions,
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeString($this->name);
        $writer->writeArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readString();
        $partitionsCount = $reader->readArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = OffsetDeleteRequestTopic\OffsetDeleteRequestPartition::decode($reader, $version);
            $partitions[] = $item;
        }
        return new self($name, $partitions);
    }
}
