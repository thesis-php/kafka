<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<TxnOffsetCommitResponse>
 */
final readonly class TxnOffsetCommitRequest implements Request
{
    /**
     * @param string $transactionalId The ID of the transaction.
     * @param string $groupId The ID of the group.
     * @param int $producerId The current producer ID in use by the transactional ID.
     * @param int $producerEpoch The current epoch associated with the producer ID.
     * @param int $generationId The generation of the consumer.
     * @param string $memberId The member ID assigned by the group coordinator.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param list<TxnOffsetCommitRequest\TxnOffsetCommitRequestTopic> $topics Each topic that we want to commit offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public string $groupId,
        public int $producerId,
        public int $producerEpoch,
        public int $generationId,
        public string $memberId,
        public ?string $groupInstanceId,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::TxnOffsetCommit;
    }

    #[\Override]
    public function responseType(): string
    {
        return TxnOffsetCommitResponse::class;
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
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        $writer->writeInt64($this->producerId);
        $writer->writeInt16($this->producerEpoch);
        if ($version >= 3) {
            $writer->writeInt32($this->generationId);
        }
        if ($version >= 3) {
            $writer->writeCompactString($this->memberId);
        }
        if ($version >= 3) {
            $writer->writeCompactNullableString($this->groupInstanceId);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
