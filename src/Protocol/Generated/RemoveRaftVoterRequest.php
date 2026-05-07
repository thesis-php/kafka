<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<RemoveRaftVoterResponse>
 */
final readonly class RemoveRaftVoterRequest implements Request
{
    /**
     * @param ?string $clusterId The cluster id of the request.
     * @param int $voterId The replica id of the voter getting removed from the topic partition.
     * @param string $voterDirectoryId The directory id of the voter getting removed from the topic partition.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $voterId,
        public string $voterDirectoryId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::RemoveRaftVoter;
    }

    #[\Override]
    public function responseType(): string
    {
        return RemoveRaftVoterResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->clusterId);
        $writer->writeInt32($this->voterId);
        $writer->writeUuid($this->voterDirectoryId);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
