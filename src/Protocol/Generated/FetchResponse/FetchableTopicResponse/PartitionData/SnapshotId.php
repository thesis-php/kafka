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
final readonly class SnapshotId implements Encodable, Decodable
{
    /**
     * @param int $endOffset The end offset of the epoch.
     * @param int $epoch The largest epoch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $endOffset,
        public int $epoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt64($this->endOffset);
        $writer->writeInt32($this->epoch);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $endOffset = $reader->readInt64();
        $epoch = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($endOffset, $epoch, $unknownTags);
    }
}
