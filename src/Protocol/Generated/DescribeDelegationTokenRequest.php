<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeDelegationTokenResponse>
 */
final readonly class DescribeDelegationTokenRequest implements Request
{
    /**
     * @param ?list<DescribeDelegationTokenRequest\DescribeDelegationTokenOwner> $owners Each owner that we want to describe delegation tokens for, or null to describe all tokens.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?array $owners,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeDelegationToken;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeDelegationTokenResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactNullableArrayLength($this->owners === null ? null : \count($this->owners));
        } else {
            $writer->writeNullableArrayLength($this->owners === null ? null : \count($this->owners));
        }
        if ($this->owners !== null) {
            foreach ($this->owners as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}
