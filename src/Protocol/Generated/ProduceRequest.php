<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ProduceResponse>
 */
final readonly class ProduceRequest implements Request
{
    /**
     * @param ?string $transactionalId The transactional ID, or null if the producer is not transactional.
     * @param int $acks The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
     * @param int $timeoutMs The timeout to await a response in milliseconds.
     * @param list<ProduceRequest\TopicProduceData> $topicData Each topic to produce to.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $transactionalId,
        public int $acks,
        public int $timeoutMs,
        public array $topicData,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Produce;
    }

    #[\Override]
    public function responseType(): string
    {
        return ProduceResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        if ($flexible) {
            $writer->writeCompactNullableString($this->transactionalId);
        } else {
            $writer->writeNullableString($this->transactionalId);
        }
        $writer->writeInt16($this->acks);
        $writer->writeInt32($this->timeoutMs);
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topicData));
        } else {
            $writer->writeArrayLength(\count($this->topicData));
        }
        foreach ($this->topicData as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
