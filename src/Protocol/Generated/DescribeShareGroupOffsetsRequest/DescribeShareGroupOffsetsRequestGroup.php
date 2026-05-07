<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeShareGroupOffsetsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsRequestGroup implements Encodable, Decodable
{
    /**
     * @param string $groupId The group identifier.
     * @param ?list<DescribeShareGroupOffsetsRequestGroup\DescribeShareGroupOffsetsRequestTopic> $topics The topics to describe offsets for, or null for all topic-partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public ?array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->groupId);
        $writer->writeCompactNullableArrayLength($this->topics === null ? null : \count($this->topics));
        if ($this->topics !== null) {
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $groupId = $reader->readCompactString();
        $topicsCount = $reader->readCompactNullableArrayLength();
        if ($topicsCount === null) {
            $topics = null;
        } else {
            $topics = [];
            for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
                $item = DescribeShareGroupOffsetsRequestGroup\DescribeShareGroupOffsetsRequestTopic::decode($reader, $version);
                $topics[] = $item;
            }
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($groupId, $topics, $unknownTags);
    }
}
