<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<CreateDelegationTokenResponse>
 */
final readonly class CreateDelegationTokenRequest implements Request
{
    /**
     * @param ?string $ownerPrincipalType The principal type of the owner of the token. If it's null it defaults to the token request principal.
     * @param ?string $ownerPrincipalName The principal name of the owner of the token. If it's null it defaults to the token request principal.
     * @param list<CreateDelegationTokenRequest\CreatableRenewers> $renewers A list of those who are allowed to renew this token before it expires.
     * @param int $maxLifetimeMs The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $ownerPrincipalType,
        public ?string $ownerPrincipalName,
        public array $renewers,
        public int $maxLifetimeMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::CreateDelegationToken;
    }

    #[\Override]
    public function responseType(): string
    {
        return CreateDelegationTokenResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($version >= 3) {
            $writer->writeCompactNullableString($this->ownerPrincipalType);
        }
        if ($version >= 3) {
            $writer->writeCompactNullableString($this->ownerPrincipalName);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->renewers));
        } else {
            $writer->writeArrayLength(\count($this->renewers));
        }
        foreach ($this->renewers as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt64($this->maxLifetimeMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
