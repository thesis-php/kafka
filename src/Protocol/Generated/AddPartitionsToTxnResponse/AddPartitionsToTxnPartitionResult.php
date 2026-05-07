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
final readonly class AddPartitionsToTxnPartitionResult implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition indexes.
     * @param int $partitionErrorCode The response error code.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $partitionErrorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->partitionErrorCode);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $partitionIndex = $reader->readInt32();
        $partitionErrorCode = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $partitionErrorCode, $unknownTags);
    }
}
