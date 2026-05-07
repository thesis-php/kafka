<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ProduceResponse\TopicProduceResponse\PartitionProduceResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class BatchIndexAndErrorMessage implements Encodable, Decodable
{
    /**
     * @param int $batchIndex The batch index of the record that caused the batch to be dropped.
     * @param ?string $batchIndexErrorMessage The error message of the record that caused the batch to be dropped.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $batchIndex,
        public ?string $batchIndexErrorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        $writer->writeInt32($this->batchIndex);
        if ($flexible) {
            $writer->writeCompactNullableString($this->batchIndexErrorMessage);
        } else {
            $writer->writeNullableString($this->batchIndexErrorMessage);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $batchIndex = $reader->readInt32();
        if ($flexible) {
            $batchIndexErrorMessage = $reader->readCompactNullableString();
        } else {
            $batchIndexErrorMessage = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($batchIndex, $batchIndexErrorMessage, $unknownTags);
    }
}
