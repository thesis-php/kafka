<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ListOffsetsResponse>
 */
final readonly class ListOffsetsRequest implements Request
{
    /**
     * @param int $replicaId The broker ID of the requester, or -1 if this request is being made by a normal consumer.
     * @param int $isolationLevel This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
     * @param list<ListOffsetsRequest\ListOffsetsTopic> $topics Each topic in the request.
     * @param int $timeoutMs The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $replicaId,
        public int $isolationLevel,
        public array $topics,
        public int $timeoutMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ListOffsets;
    }

    #[\Override]
    public function responseType(): string
    {
        return ListOffsetsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        $writer->writeInt32($this->replicaId);
        if ($version >= 2) {
            $writer->writeInt8($this->isolationLevel);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 10) {
            $writer->writeInt32($this->timeoutMs);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
