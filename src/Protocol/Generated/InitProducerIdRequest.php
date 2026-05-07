<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<InitProducerIdResponse>
 */
final readonly class InitProducerIdRequest implements Request
{
    /**
     * @param ?string $transactionalId The transactional id, or null if the producer is not transactional.
     * @param int $transactionTimeoutMs The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
     * @param int $producerId The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
     * @param int $producerEpoch The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
     * @param bool $enable2Pc True if the client wants to enable two-phase commit (2PC) protocol for transactions.
     * @param bool $keepPreparedTxn True if the client wants to keep the currently ongoing transaction instead of aborting it.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $transactionalId,
        public int $transactionTimeoutMs,
        public int $producerId,
        public int $producerEpoch,
        public bool $enable2Pc,
        public bool $keepPreparedTxn,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::InitProducerId;
    }

    #[\Override]
    public function responseType(): string
    {
        return InitProducerIdResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactNullableString($this->transactionalId);
        } else {
            $writer->writeNullableString($this->transactionalId);
        }
        $writer->writeInt32($this->transactionTimeoutMs);
        if ($version >= 3) {
            $writer->writeInt64($this->producerId);
        }
        if ($version >= 3) {
            $writer->writeInt16($this->producerEpoch);
        }
        if ($version >= 6) {
            $writer->writeBool($this->enable2Pc);
        }
        if ($version >= 6) {
            $writer->writeBool($this->keepPreparedTxn);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
