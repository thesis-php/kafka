<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetFetchResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchResponseGroup implements Encodable, Decodable
{
    /**
     * @param string $groupId The group ID.
     * @param list<OffsetFetchResponseGroup\OffsetFetchResponseTopics> $topics The responses per topic.
     * @param int $errorCode The group-level error code, or 0 if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public array $topics,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeCompactString($this->groupId);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $groupId = $reader->readCompactString();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = OffsetFetchResponseGroup\OffsetFetchResponseTopics::decode($reader, $version);
            $topics[] = $item;
        }
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($groupId, $topics, $errorCode, $unknownTags);
    }
}
