<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AddPartitionsToTxnResponse>
 */
final readonly class AddPartitionsToTxnRequest implements Request
{
    /**
     * @param list<AddPartitionsToTxnRequest\AddPartitionsToTxnTransaction> $transactions List of transactions to add partitions to.
     * @param string $v3AndBelowTransactionalId The transactional id corresponding to the transaction.
     * @param int $v3AndBelowProducerId Current producer id in use by the transactional id.
     * @param int $v3AndBelowProducerEpoch Current epoch associated with the producer id.
     * @param list<AddPartitionsToTxnRequest\AddPartitionsToTxnTopic> $v3AndBelowTopics The partitions to add to the transaction.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $transactions,
        public string $v3AndBelowTransactionalId,
        public int $v3AndBelowProducerId,
        public int $v3AndBelowProducerEpoch,
        public array $v3AndBelowTopics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AddPartitionsToTxn;
    }

    #[\Override]
    public function responseType(): string
    {
        return AddPartitionsToTxnResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($version >= 4) {
            $writer->writeCompactArrayLength(\count($this->transactions));
            foreach ($this->transactions as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version <= 3) {
            if ($flexible) {
                $writer->writeCompactString($this->v3AndBelowTransactionalId);
            } else {
                $writer->writeString($this->v3AndBelowTransactionalId);
            }
        }
        if ($version <= 3) {
            $writer->writeInt64($this->v3AndBelowProducerId);
        }
        if ($version <= 3) {
            $writer->writeInt16($this->v3AndBelowProducerEpoch);
        }
        if ($version <= 3) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->v3AndBelowTopics));
            } else {
                $writer->writeArrayLength(\count($this->v3AndBelowTopics));
            }
            foreach ($this->v3AndBelowTopics as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
