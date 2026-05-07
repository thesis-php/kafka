<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<EndTxnResponse>
 */
final readonly class EndTxnRequest implements Request
{
    /**
     * @param string $transactionalId The ID of the transaction to end.
     * @param int $producerId The producer ID.
     * @param int $producerEpoch The current epoch associated with the producer.
     * @param bool $committed True if the transaction was committed, false if it was aborted.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public int $producerId,
        public int $producerEpoch,
        public bool $committed,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::EndTxn;
    }

    #[\Override]
    public function responseType(): string
    {
        return EndTxnResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($flexible) {
            $writer->writeCompactString($this->transactionalId);
        } else {
            $writer->writeString($this->transactionalId);
        }
        $writer->writeInt64($this->producerId);
        $writer->writeInt16($this->producerEpoch);
        $writer->writeBool($this->committed);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
