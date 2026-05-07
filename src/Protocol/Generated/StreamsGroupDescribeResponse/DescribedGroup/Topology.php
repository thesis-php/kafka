<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse\DescribedGroup;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Topology implements Encodable, Decodable
{
    /**
     * @param int $epoch The epoch of the currently initialized topology for this group.
     * @param ?list<Topology\Subtopology> $subtopologies The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $epoch,
        public ?array $subtopologies,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->epoch);
        $writer->writeCompactNullableArrayLength($this->subtopologies === null ? null : \count($this->subtopologies));
        if ($this->subtopologies !== null) {
            foreach ($this->subtopologies as $item) {
                $item->encode($writer, $version);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $epoch = $reader->readInt32();
        $subtopologiesCount = $reader->readCompactNullableArrayLength();
        if ($subtopologiesCount === null) {
            $subtopologies = null;
        } else {
            $subtopologies = [];
            for ($subtopologiesIndex = 0; $subtopologiesIndex < $subtopologiesCount; ++$subtopologiesIndex) {
                $item = Topology\Subtopology::decode($reader, $version);
                $subtopologies[] = $item;
            }
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($epoch, $subtopologies, $unknownTags);
    }
}
