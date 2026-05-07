<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class RenewDelegationTokenResponse implements Response
{
    /**
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $expiryTimestampMs The timestamp in milliseconds at which this token expires.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public int $expiryTimestampMs,
        public int $throttleTimeMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $errorCode = $reader->readInt16();
        $expiryTimestampMs = $reader->readInt64();
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $expiryTimestampMs, $throttleTimeMs, $unknownTags);
    }
}
