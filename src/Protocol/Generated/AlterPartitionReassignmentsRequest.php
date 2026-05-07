<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterPartitionReassignmentsResponse>
 */
final readonly class AlterPartitionReassignmentsRequest implements Request
{
    /**
     * @param int $timeoutMs The time in ms to wait for the request to complete.
     * @param bool $allowReplicationFactorChange The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
     * @param list<AlterPartitionReassignmentsRequest\ReassignableTopic> $topics The topics to reassign.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $timeoutMs,
        public bool $allowReplicationFactorChange,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterPartitionReassignments;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterPartitionReassignmentsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->timeoutMs);
        if ($version >= 1) {
            $writer->writeBool($this->allowReplicationFactorChange);
        }
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
