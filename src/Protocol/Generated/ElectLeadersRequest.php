<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ElectLeadersResponse>
 */
final readonly class ElectLeadersRequest implements Request
{
    /**
     * @param int $electionType Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
     * @param ?list<ElectLeadersRequest\TopicPartitions> $topicPartitions The topic partitions to elect leaders.
     * @param int $timeoutMs The time in ms to wait for the election to complete.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $electionType,
        public ?array $topicPartitions,
        public int $timeoutMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ElectLeaders;
    }

    #[\Override]
    public function responseType(): string
    {
        return ElectLeadersResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($version >= 1) {
            $writer->writeInt8($this->electionType);
        }
        if ($flexible) {
            $writer->writeCompactNullableArrayLength($this->topicPartitions === null ? null : \count($this->topicPartitions));
        } else {
            $writer->writeNullableArrayLength($this->topicPartitions === null ? null : \count($this->topicPartitions));
        }
        if ($this->topicPartitions !== null) {
            foreach ($this->topicPartitions as $item) {
                $item->encode($writer, $version);
            }
        }
        $writer->writeInt32($this->timeoutMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
