<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetFetchRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetFetchRequestGroup implements Encodable, Decodable
{
    /**
     * @param string $groupId The group ID.
     * @param ?string $memberId The member id.
     * @param int $memberEpoch The member epoch if using the new consumer protocol (KIP-848).
     * @param ?list<OffsetFetchRequestGroup\OffsetFetchRequestTopics> $topics Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public ?string $memberId,
        public int $memberEpoch,
        public ?array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeCompactString($this->groupId);
        if ($version >= 9) {
            $writer->writeCompactNullableString($this->memberId);
        }
        if ($version >= 9) {
            $writer->writeInt32($this->memberEpoch);
        }
        $writer->writeCompactNullableArrayLength($this->topics === null ? null : \count($this->topics));
        if ($this->topics !== null) {
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        $groupId = $reader->readCompactString();
        $memberId = null;
        if ($version >= 9) {
            $memberId = $reader->readCompactNullableString();
        }
        $memberEpoch = -1;
        if ($version >= 9) {
            $memberEpoch = $reader->readInt32();
        }
        $topicsCount = $reader->readCompactNullableArrayLength();
        if ($topicsCount === null) {
            $topics = null;
        } else {
            $topics = [];
            for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
                $item = OffsetFetchRequestGroup\OffsetFetchRequestTopics::decode($reader, $version);
                $topics[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($groupId, $memberId, $memberEpoch, $topics, $unknownTags);
    }
}
