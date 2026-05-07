<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeProducersResponse\TopicResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionResponse implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $errorCode The partition error code, or 0 if there was no error.
     * @param ?string $errorMessage The partition error message, which may be null if no additional details are available.
     * @param list<PartitionResponse\ProducerState> $activeProducers The active producers for the partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public ?string $errorMessage,
        public array $activeProducers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeCompactArrayLength(\count($this->activeProducers));
        foreach ($this->activeProducers as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $activeProducersCount = $reader->readCompactArrayLength();
        $activeProducers = [];
        for ($activeProducersIndex = 0; $activeProducersIndex < $activeProducersCount; ++$activeProducersIndex) {
            $item = PartitionResponse\ProducerState::decode($reader, $version);
            $activeProducers[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($partitionIndex, $errorCode, $errorMessage, $activeProducers, $unknownTags);
    }
}
