<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ListPartitionReassignmentsResponse>
 */
final readonly class ListPartitionReassignmentsRequest implements Request
{
    /**
     * @param int $timeoutMs The time in ms to wait for the request to complete.
     * @param ?list<ListPartitionReassignmentsRequest\ListPartitionReassignmentsTopics> $topics The topics to list partition reassignments for, or null to list everything.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $timeoutMs,
        public ?array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ListPartitionReassignments;
    }

    #[\Override]
    public function responseType(): string
    {
        return ListPartitionReassignmentsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->timeoutMs);
        $writer->writeCompactNullableArrayLength($this->topics === null ? null : \count($this->topics));
        if ($this->topics !== null) {
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
