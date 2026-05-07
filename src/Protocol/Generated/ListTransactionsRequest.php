<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ListTransactionsResponse>
 */
final readonly class ListTransactionsRequest implements Request
{
    /**
     * @param list<string> $stateFilters The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
     * @param list<int> $producerIdFilters The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
     * @param int $durationFilter Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
     * @param ?string $transactionalIdPattern The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $stateFilters,
        public array $producerIdFilters,
        public int $durationFilter,
        public ?string $transactionalIdPattern,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ListTransactions;
    }

    #[\Override]
    public function responseType(): string
    {
        return ListTransactionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->stateFilters));
        foreach ($this->stateFilters as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeCompactArrayLength(\count($this->producerIdFilters));
        foreach ($this->producerIdFilters as $item) {
            $writer->writeInt64($item);
        }
        if ($version >= 1) {
            $writer->writeInt64($this->durationFilter);
        }
        if ($version >= 2) {
            $writer->writeCompactNullableString($this->transactionalIdPattern);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}
