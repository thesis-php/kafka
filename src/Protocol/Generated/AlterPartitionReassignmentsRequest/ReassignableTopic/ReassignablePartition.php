<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterPartitionReassignmentsRequest\ReassignableTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ReassignablePartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param ?list<int> $replicas The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public ?array $replicas,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeCompactNullableArrayLength($this->replicas === null ? null : \count($this->replicas));
        if ($this->replicas !== null) {
            foreach ($this->replicas as $item) {
                $writer->writeInt32($item);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $replicasCount = $reader->readCompactNullableArrayLength();
        if ($replicasCount === null) {
            $replicas = null;
        } else {
            $replicas = [];
            for ($replicasIndex = 0; $replicasIndex < $replicasCount; ++$replicasIndex) {
                $item = $reader->readInt32();
                $replicas[] = $item;
            }
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $replicas, $unknownTags);
    }
}
