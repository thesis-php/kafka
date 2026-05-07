<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeShareGroupOffsetsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeShareGroupOffsetsResponseGroup implements Encodable, Decodable
{
    /**
     * @param string $groupId The group identifier.
     * @param list<DescribeShareGroupOffsetsResponseGroup\DescribeShareGroupOffsetsResponseTopic> $topics The results for each topic.
     * @param int $errorCode The group-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The group-level error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public array $topics,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->groupId);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $groupId = $reader->readCompactString();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = DescribeShareGroupOffsetsResponseGroup\DescribeShareGroupOffsetsResponseTopic::decode($reader, $version);
            $topics[] = $item;
        }
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($groupId, $topics, $errorCode, $errorMessage, $unknownTags);
    }
}
