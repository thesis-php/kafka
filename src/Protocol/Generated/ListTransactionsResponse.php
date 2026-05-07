<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ListTransactionsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param list<string> $unknownStateFilters Set of state filters provided in the request which were unknown to the transaction coordinator.
     * @param list<ListTransactionsResponse\TransactionState> $transactionStates The current state of the transaction for the transactional id.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $unknownStateFilters,
        public array $transactionStates,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $unknownStateFiltersCount = $reader->readCompactArrayLength();
        $unknownStateFilters = [];
        for ($unknownStateFiltersIndex = 0; $unknownStateFiltersIndex < $unknownStateFiltersCount; ++$unknownStateFiltersIndex) {
            $item = $reader->readCompactString();
            $unknownStateFilters[] = $item;
        }
        $transactionStatesCount = $reader->readCompactArrayLength();
        $transactionStates = [];
        for ($transactionStatesIndex = 0; $transactionStatesIndex < $transactionStatesCount; ++$transactionStatesIndex) {
            $item = ListTransactionsResponse\TransactionState::decode($reader, $version);
            $transactionStates[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $unknownStateFilters, $transactionStates, $unknownTags);
    }
}
