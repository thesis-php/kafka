<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ExpireDelegationTokenResponse>
 */
final readonly class ExpireDelegationTokenRequest implements Request
{
    /**
     * @param string $hmac The HMAC of the delegation token to be expired.
     * @param int $expiryTimePeriodMs The expiry time period in milliseconds.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $hmac,
        public int $expiryTimePeriodMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ExpireDelegationToken;
    }

    #[\Override]
    public function responseType(): string
    {
        return ExpireDelegationTokenResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactBytes($this->hmac);
        } else {
            $writer->writeBytes($this->hmac);
        }
        $writer->writeInt64($this->expiryTimePeriodMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
