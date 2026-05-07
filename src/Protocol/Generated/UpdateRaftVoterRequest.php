<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<UpdateRaftVoterResponse>
 */
final readonly class UpdateRaftVoterRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id.
     * @param int $currentLeaderEpoch The current leader epoch of the partition, -1 for unknown leader epoch.
     * @param int $voterId The replica id of the voter getting updated in the topic partition.
     * @param UuidInterface $voterDirectoryId The directory id of the voter getting updated in the topic partition.
     * @param list<UpdateRaftVoterRequest\Listener> $listeners The endpoint that can be used to communicate with the leader.
     * @param UpdateRaftVoterRequest\KRaftVersionFeature $kRaftVersionFeature The range of versions of the protocol that the replica supports.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $currentLeaderEpoch,
        public int $voterId,
        public UuidInterface $voterDirectoryId,
        public array $listeners,
        public UpdateRaftVoterRequest\KRaftVersionFeature $kRaftVersionFeature,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::UpdateRaftVoter;
    }

    #[\Override]
    public function responseType(): string
    {
        return UpdateRaftVoterResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->clusterId);
        $writer->writeInt32($this->currentLeaderEpoch);
        $writer->writeInt32($this->voterId);
        $writer->writeUuid($this->voterDirectoryId);
        $writer->writeCompactArrayLength(\count($this->listeners));
        foreach ($this->listeners as $item) {
            $item->encode($writer, $version);
        }
        $this->kRaftVersionFeature->encode($writer, $version);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
