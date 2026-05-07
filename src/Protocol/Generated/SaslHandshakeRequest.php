<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<SaslHandshakeResponse>
 */
final readonly class SaslHandshakeRequest implements Request
{
    /**
     * @param string $mechanism The SASL mechanism chosen by the client.
     */
    public function __construct(
        public string $mechanism,
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::SaslHandshake;
    }

    #[\Override]
    public function responseType(): string
    {
        return SaslHandshakeResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeString($this->mechanism);
    }
}
