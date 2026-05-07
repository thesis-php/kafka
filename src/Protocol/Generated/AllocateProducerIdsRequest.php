<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AllocateProducerIdsResponse>
 */
final readonly class AllocateProducerIdsRequest implements Request
{
    /**
     * @param int $brokerId The ID of the requesting broker.
     * @param int $brokerEpoch The epoch of the requesting broker.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public int $brokerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AllocateProducerIds;
    }

    #[\Override]
    public function responseType(): string
    {
        return AllocateProducerIdsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeInt64($this->brokerEpoch);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
