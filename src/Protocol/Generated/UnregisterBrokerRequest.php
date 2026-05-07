<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<UnregisterBrokerResponse>
 */
final readonly class UnregisterBrokerRequest implements Request
{
    /**
     * @param int $brokerId The broker ID to unregister.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::UnregisterBroker;
    }

    #[\Override]
    public function responseType(): string
    {
        return UnregisterBrokerResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
