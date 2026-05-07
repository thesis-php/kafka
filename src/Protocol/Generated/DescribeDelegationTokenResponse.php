<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeDelegationTokenResponse implements Response
{
    /**
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param list<DescribeDelegationTokenResponse\DescribedDelegationToken> $tokens The tokens.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public array $tokens,
        public int $throttleTimeMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $tokensCount = $reader->readCompactArrayLength();
        } else {
            $tokensCount = $reader->readArrayLength();
        }
        $tokens = [];
        for ($tokensIndex = 0; $tokensIndex < $tokensCount; ++$tokensIndex) {
            $item = DescribeDelegationTokenResponse\DescribedDelegationToken::decode($reader, $version);
            $tokens[] = $item;
        }
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $tokens, $throttleTimeMs, $unknownTags);
    }
}
