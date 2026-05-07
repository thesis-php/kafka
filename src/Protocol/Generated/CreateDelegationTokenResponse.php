<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class CreateDelegationTokenResponse implements Response
{
    /**
     * @param int $errorCode The top-level error, or zero if there was no error.
     * @param string $principalType The principal type of the token owner.
     * @param string $principalName The name of the token owner.
     * @param string $tokenRequesterPrincipalType The principal type of the requester of the token.
     * @param string $tokenRequesterPrincipalName The principal type of the requester of the token.
     * @param int $issueTimestampMs When this token was generated.
     * @param int $expiryTimestampMs When this token expires.
     * @param int $maxTimestampMs The maximum lifetime of this token.
     * @param string $tokenId The token UUID.
     * @param string $hmac HMAC of the delegation token.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public string $principalType,
        public string $principalName,
        public string $tokenRequesterPrincipalType,
        public string $tokenRequesterPrincipalName,
        public int $issueTimestampMs,
        public int $expiryTimestampMs,
        public int $maxTimestampMs,
        public string $tokenId,
        public string $hmac,
        public int $throttleTimeMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $principalType = $reader->readCompactString();
        } else {
            $principalType = $reader->readString();
        }
        if ($flexible) {
            $principalName = $reader->readCompactString();
        } else {
            $principalName = $reader->readString();
        }
        $tokenRequesterPrincipalType = '';
        if ($version >= 3) {
            $tokenRequesterPrincipalType = $reader->readCompactString();
        }
        $tokenRequesterPrincipalName = '';
        if ($version >= 3) {
            $tokenRequesterPrincipalName = $reader->readCompactString();
        }
        $issueTimestampMs = $reader->readInt64();
        $expiryTimestampMs = $reader->readInt64();
        $maxTimestampMs = $reader->readInt64();
        if ($flexible) {
            $tokenId = $reader->readCompactString();
        } else {
            $tokenId = $reader->readString();
        }
        if ($flexible) {
            $hmac = $reader->readCompactBytes();
        } else {
            $hmac = $reader->readBytes();
        }
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $principalType, $principalName, $tokenRequesterPrincipalType, $tokenRequesterPrincipalName, $issueTimestampMs, $expiryTimestampMs, $maxTimestampMs, $tokenId, $hmac, $throttleTimeMs, $unknownTags);
    }
}
