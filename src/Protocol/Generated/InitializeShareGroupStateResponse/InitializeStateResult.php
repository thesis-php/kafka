<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\InitializeShareGroupStateResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class InitializeStateResult implements Encodable, Decodable
{
    /**
     * @param string $topicId The topic identifier.
     * @param list<InitializeStateResult\PartitionResult> $partitions The results for the partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicId,
        public array $partitions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeUuid($this->topicId);
        $writer->writeCompactArrayLength(\count($this->partitions));
        foreach ($this->partitions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicId = $reader->readUuid();
        $partitionsCount = $reader->readCompactArrayLength();
        $partitions = [];
        for ($partitionsIndex = 0; $partitionsIndex < $partitionsCount; ++$partitionsIndex) {
            $item = InitializeStateResult\PartitionResult::decode($reader, $version);
            $partitions[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($topicId, $partitions, $unknownTags);
    }
}
