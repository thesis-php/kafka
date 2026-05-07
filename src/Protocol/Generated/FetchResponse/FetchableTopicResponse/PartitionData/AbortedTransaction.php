<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchResponse\FetchableTopicResponse\PartitionData;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AbortedTransaction implements Encodable, Decodable
{
    /**
     * @param int $producerId The producer id associated with the aborted transaction.
     * @param int $firstOffset The first offset in the aborted transaction.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $producerId,
        public int $firstOffset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt64($this->producerId);
        $writer->writeInt64($this->firstOffset);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $producerId = $reader->readInt64();
        $firstOffset = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($producerId, $firstOffset, $unknownTags);
    }
}
