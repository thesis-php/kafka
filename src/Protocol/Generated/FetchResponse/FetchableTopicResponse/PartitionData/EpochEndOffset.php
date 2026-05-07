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
final readonly class EpochEndOffset implements Encodable, Decodable
{
    /**
     * @param int $epoch The largest epoch.
     * @param int $endOffset The end offset of the epoch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $epoch,
        public int $endOffset,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt32($this->epoch);
        $writer->writeInt64($this->endOffset);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $epoch = $reader->readInt32();
        $endOffset = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($epoch, $endOffset, $unknownTags);
    }
}
