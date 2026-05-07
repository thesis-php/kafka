<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DeleteRecordsResponse>
 */
final readonly class DeleteRecordsRequest implements Request
{
    /**
     * @param list<DeleteRecordsRequest\DeleteRecordsTopic> $topics Each topic that we want to delete records from.
     * @param int $timeoutMs How long to wait for the deletion to complete, in milliseconds.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topics,
        public int $timeoutMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DeleteRecords;
    }

    #[\Override]
    public function responseType(): string
    {
        return DeleteRecordsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->timeoutMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
