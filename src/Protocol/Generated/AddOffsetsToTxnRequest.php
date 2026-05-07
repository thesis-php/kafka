<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AddOffsetsToTxnResponse>
 */
final readonly class AddOffsetsToTxnRequest implements Request
{
    /**
     * @param string $transactionalId The transactional id corresponding to the transaction.
     * @param int $producerId Current producer id in use by the transactional id.
     * @param int $producerEpoch Current epoch associated with the producer id.
     * @param string $groupId The unique group identifier.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public int $producerId,
        public int $producerEpoch,
        public string $groupId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AddOffsetsToTxn;
    }

    #[\Override]
    public function responseType(): string
    {
        return AddOffsetsToTxnResponse::class;
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
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
