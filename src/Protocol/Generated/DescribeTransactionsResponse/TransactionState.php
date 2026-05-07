<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeTransactionsResponse;

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
     * @param int $errorCode The error code.
     * @param string $transactionalId The transactional id.
     * @param string $transactionState The current transaction state of the producer.
     * @param int $transactionTimeoutMs The timeout in milliseconds for the transaction.
     * @param int $transactionStartTimeMs The start time of the transaction in milliseconds.
     * @param int $producerId The current producer id associated with the transaction.
     * @param int $producerEpoch The current epoch associated with the producer id.
     * @param list<TransactionState\TopicData> $topics The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public string $transactionalId,
        public string $transactionState,
        public int $transactionTimeoutMs,
        public int $transactionStartTimeMs,
        public int $producerId,
        public int $producerEpoch,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactString($this->transactionalId);
        $writer->writeCompactString($this->transactionState);
        $writer->writeInt32($this->transactionTimeoutMs);
        $writer->writeInt64($this->transactionStartTimeMs);
        $writer->writeInt64($this->producerId);
        $writer->writeInt16($this->producerEpoch);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $transactionalId = $reader->readCompactString();
        $transactionState = $reader->readCompactString();
        $transactionTimeoutMs = $reader->readInt32();
        $transactionStartTimeMs = $reader->readInt64();
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt16();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = TransactionState\TopicData::decode($reader, $version);
            $topics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($errorCode, $transactionalId, $transactionState, $transactionTimeoutMs, $transactionStartTimeMs, $producerId, $producerEpoch, $topics, $unknownTags);
    }
}
