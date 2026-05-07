<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ListTransactionsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TransactionState implements Encodable, Decodable
{
    /**
     * @param string $transactionalId The transactional id.
     * @param int $producerId The producer id.
     * @param string $transactionState The current transaction state of the producer.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public int $producerId,
        public string $transactionState,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->transactionalId);
        $writer->writeInt64($this->producerId);
        $writer->writeCompactString($this->transactionState);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $transactionalId = $reader->readCompactString();
        $producerId = $reader->readInt64();
        $transactionState = $reader->readCompactString();
        $unknownTags = $reader->readTaggedFields();
        return new self($transactionalId, $producerId, $transactionState, $unknownTags);
    }
}
