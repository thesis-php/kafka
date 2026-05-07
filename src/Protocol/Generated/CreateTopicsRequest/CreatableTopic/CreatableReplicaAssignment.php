<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateTopicsRequest\CreatableTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatableReplicaAssignment implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param list<int> $brokerIds The brokers to place the partition on.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public array $brokerIds,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        $writer->writeInt32($this->partitionIndex);
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->brokerIds));
        } else {
            $writer->writeArrayLength(\count($this->brokerIds));
        }
        foreach ($this->brokerIds as $item) {
            $writer->writeInt32($item);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        $partitionIndex = $reader->readInt32();
        if ($flexible) {
            $brokerIdsCount = $reader->readCompactArrayLength();
        } else {
            $brokerIdsCount = $reader->readArrayLength();
        }
        $brokerIds = [];
        for ($brokerIdsIndex = 0; $brokerIdsIndex < $brokerIdsCount; ++$brokerIdsIndex) {
            $item = $reader->readInt32();
            $brokerIds[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $brokerIds, $unknownTags);
    }
}
