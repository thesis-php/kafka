<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<GetTelemetrySubscriptionsResponse>
 */
final readonly class GetTelemetrySubscriptionsRequest implements Request
{
    /**
     * @param string $clientInstanceId Unique id for this client instance, must be set to 0 on the first request.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $clientInstanceId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::GetTelemetrySubscriptions;
    }

    #[\Override]
    public function responseType(): string
    {
        return GetTelemetrySubscriptionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeUuid($this->clientInstanceId);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
