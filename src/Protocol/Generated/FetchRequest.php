<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<FetchResponse>
 */
final readonly class FetchRequest implements Request
{
    /**
     * @param ?string $clusterId The clusterId if known. This is used to validate metadata fetches prior to broker registration.
     * @param int $replicaId The broker ID of the follower, of -1 if this request is from a consumer.
     * @param FetchRequest\ReplicaState $replicaState The state of the replica in the follower.
     * @param int $maxWaitMs The maximum time in milliseconds to wait for the response.
     * @param int $minBytes The minimum bytes to accumulate in the response.
     * @param int $maxBytes The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
     * @param int $isolationLevel This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
     * @param int $sessionId The fetch session ID.
     * @param int $sessionEpoch The fetch session epoch, which is used for ordering requests in a session.
     * @param list<FetchRequest\FetchTopic> $topics The topics to fetch.
     * @param list<FetchRequest\ForgottenTopic> $forgottenTopicsData In an incremental fetch request, the partitions to remove.
     * @param string $rackId Rack ID of the consumer making this request.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $clusterId,
        public int $replicaId,
        public FetchRequest\ReplicaState $replicaState,
        public int $maxWaitMs,
        public int $minBytes,
        public int $maxBytes,
        public int $isolationLevel,
        public int $sessionId,
        public int $sessionEpoch,
        public array $topics,
        public array $forgottenTopicsData,
        public string $rackId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Fetch;
    }

    #[\Override]
    public function responseType(): string
    {
        return FetchResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        if ($version <= 14) {
            $writer->writeInt32($this->replicaId);
        }
        $writer->writeInt32($this->maxWaitMs);
        $writer->writeInt32($this->minBytes);
        $writer->writeInt32($this->maxBytes);
        $writer->writeInt8($this->isolationLevel);
        if ($version >= 7) {
            $writer->writeInt32($this->sessionId);
        }
        if ($version >= 7) {
            $writer->writeInt32($this->sessionEpoch);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 7) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->forgottenTopicsData));
            } else {
                $writer->writeArrayLength(\count($this->forgottenTopicsData));
            }
            foreach ($this->forgottenTopicsData as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 11) {
            if ($flexible) {
                $writer->writeCompactString($this->rackId);
            } else {
                $writer->writeString($this->rackId);
            }
        }
        if ($flexible) {
            $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
            if (isset($taggedFields[0])) {
                throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'ClusterId\'.');
            }
            $tagWriter = new Writer();
            $tagWriter->writeCompactNullableString($this->clusterId);
            $taggedFields[0] = (string) $tagWriter;
            if ($version >= 15) {
                if (isset($taggedFields[1])) {
                    throw new \UnexpectedValueException('Unknown tag \'1\' collides with known field \'ReplicaState\'.');
                }
                $tagWriter = new Writer();
                $this->replicaState->encode($tagWriter, $version);
                $taggedFields[1] = (string) $tagWriter;
            }
            $writer->writeTaggedFields($taggedFields);
        }
    }
}
