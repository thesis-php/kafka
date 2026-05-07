<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<RenewDelegationTokenResponse>
 */
final readonly class RenewDelegationTokenRequest implements Request
{
    /**
     * @param string $hmac The HMAC of the delegation token to be renewed.
     * @param int $renewPeriodMs The renewal time period in milliseconds.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $hmac,
        public int $renewPeriodMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::RenewDelegationToken;
    }

    #[\Override]
    public function responseType(): string
    {
        return RenewDelegationTokenResponse::class;
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
        $writer->writeInt64($this->renewPeriodMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
