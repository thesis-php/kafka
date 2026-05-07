<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ShareFetchResponse>
 */
final readonly class ShareFetchRequest implements Request
{
    /**
     * @param ?string $groupId The group identifier.
     * @param ?string $memberId The member ID.
     * @param int $shareSessionEpoch The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
     * @param int $maxWaitMs The maximum time in milliseconds to wait for the response.
     * @param int $minBytes The minimum bytes to accumulate in the response.
     * @param int $maxBytes The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
     * @param int $maxRecords The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
     * @param int $batchSize The optimal number of records for batches of acquired records and acknowledgements.
     * @param int $shareAcquireMode The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
     * @param bool $isRenewAck Whether Renew type acknowledgements present in AcknowledgementBatches.
     * @param list<ShareFetchRequest\FetchTopic> $topics The topics to fetch.
     * @param list<ShareFetchRequest\ForgottenTopic> $forgottenTopicsData The partitions to remove from this share session.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $groupId,
        public ?string $memberId,
        public int $shareSessionEpoch,
        public int $maxWaitMs,
        public int $minBytes,
        public int $maxBytes,
        public int $maxRecords,
        public int $batchSize,
        public int $shareAcquireMode,
        public bool $isRenewAck,
        public array $topics,
        public array $forgottenTopicsData,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ShareFetch;
    }

    #[\Override]
    public function responseType(): string
    {
        return ShareFetchResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->groupId);
        $writer->writeCompactNullableString($this->memberId);
        $writer->writeInt32($this->shareSessionEpoch);
        $writer->writeInt32($this->maxWaitMs);
        $writer->writeInt32($this->minBytes);
        $writer->writeInt32($this->maxBytes);
        $writer->writeInt32($this->maxRecords);
        $writer->writeInt32($this->batchSize);
        if ($version >= 2) {
            $writer->writeInt8($this->shareAcquireMode);
        }
        if ($version >= 2) {
            $writer->writeBool($this->isRenewAck);
        }
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->forgottenTopicsData));
        foreach ($this->forgottenTopicsData as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
