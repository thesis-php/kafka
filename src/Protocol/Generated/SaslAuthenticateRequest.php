<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<SaslAuthenticateResponse>
 */
final readonly class SaslAuthenticateRequest implements Request
{
    /**
     * @param string $authBytes The SASL authentication bytes from the client, as defined by the SASL mechanism.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $authBytes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::SaslAuthenticate;
    }

    #[\Override]
    public function responseType(): string
    {
        return SaslAuthenticateResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactBytes($this->authBytes);
        } else {
            $writer->writeBytes($this->authBytes);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
