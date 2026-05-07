<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeTransactionsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<DescribeTransactionsResponse\TransactionState> $transactionStates The current state of the transaction.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $transactionStates,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $transactionStatesCount = $reader->readCompactArrayLength();
        $transactionStates = [];
        for ($transactionStatesIndex = 0; $transactionStatesIndex < $transactionStatesCount; ++$transactionStatesIndex) {
            $item = DescribeTransactionsResponse\TransactionState::decode($reader, $version);
            $transactionStates[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $transactionStates, $unknownTags);
    }
}
