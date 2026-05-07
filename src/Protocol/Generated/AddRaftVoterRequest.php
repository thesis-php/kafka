<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AddRaftVoterResponse>
 */
final readonly class AddRaftVoterRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id.
     * @param int $timeoutMs The maximum time to wait for the request to complete before returning.
     * @param int $voterId The replica id of the voter getting added to the topic partition.
     * @param string $voterDirectoryId The directory id of the voter getting added to the topic partition.
     * @param list<AddRaftVoterRequest\Listener> $listeners The endpoints that can be used to communicate with the voter.
     * @param bool $ackWhenCommitted When true, return a response after the new voter set is committed. Otherwise, return after the leader writes the changes locally.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $timeoutMs,
        public int $voterId,
        public string $voterDirectoryId,
        public array $listeners,
        public bool $ackWhenCommitted,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AddRaftVoter;
    }

    #[\Override]
    public function responseType(): string
    {
        return AddRaftVoterResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->clusterId);
        $writer->writeInt32($this->timeoutMs);
        $writer->writeInt32($this->voterId);
        $writer->writeUuid($this->voterDirectoryId);
        $writer->writeCompactArrayLength(\count($this->listeners));
        foreach ($this->listeners as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 1) {
            $writer->writeBool($this->ackWhenCommitted);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
