<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<EnvelopeResponse>
 */
final readonly class EnvelopeRequest implements Request
{
    /**
     * @param string $requestData The embedded request header and data.
     * @param ?string $requestPrincipal Value of the initial client principal when the request is redirected by a broker.
     * @param string $clientHostAddress The original client's address in bytes.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $requestData,
        public ?string $requestPrincipal,
        public string $clientHostAddress,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Envelope;
    }

    #[\Override]
    public function responseType(): string
    {
        return EnvelopeResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactBytes($this->requestData);
        $writer->writeCompactNullableBytes($this->requestPrincipal);
        $writer->writeCompactBytes($this->clientHostAddress);
        $writer->writeTaggedFields($this->unknownTags);
    }
}
