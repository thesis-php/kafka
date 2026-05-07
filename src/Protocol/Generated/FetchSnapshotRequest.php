<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<FetchSnapshotResponse>
 */
final readonly class FetchSnapshotRequest implements Request
{
    /**
     * @param ?string $clusterId The clusterId if known, this is used to validate metadata fetches prior to broker registration.
     * @param int $replicaId The broker ID of the follower.
     * @param int $maxBytes The maximum bytes to fetch from all of the snapshots.
     * @param list<FetchSnapshotRequest\TopicSnapshot> $topics The topics to fetch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $replicaId,
        public int $maxBytes,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::FetchSnapshot;
    }

    #[\Override]
    public function responseType(): string
    {
        return FetchSnapshotResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->replicaId);
        $writer->writeInt32($this->maxBytes);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
        if (isset($taggedFields[0])) {
            throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'ClusterId\'.');
        }
        $tagWriter = new Writer();
        $tagWriter->writeCompactNullableString($this->clusterId);
        $taggedFields[0] = (string) $tagWriter;
        $writer->writeTaggedFields($taggedFields);
    }
}
