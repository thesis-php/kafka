<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeDelegationTokenResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribedDelegationToken implements Encodable, Decodable
{
    /**
     * @param string $principalType The token principal type.
     * @param string $principalName The token principal name.
     * @param string $tokenRequesterPrincipalType The principal type of the requester of the token.
     * @param string $tokenRequesterPrincipalName The principal type of the requester of the token.
     * @param int $issueTimestamp The token issue timestamp in milliseconds.
     * @param int $expiryTimestamp The token expiry timestamp in milliseconds.
     * @param int $maxTimestamp The token maximum timestamp length in milliseconds.
     * @param string $tokenId The token ID.
     * @param string $hmac The token HMAC.
     * @param list<DescribedDelegationToken\DescribedDelegationTokenRenewer> $renewers Those who are able to renew this token before it expires.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $principalType,
        public string $principalName,
        public string $tokenRequesterPrincipalType,
        public string $tokenRequesterPrincipalName,
        public int $issueTimestamp,
        public int $expiryTimestamp,
        public int $maxTimestamp,
        public string $tokenId,
        public string $hmac,
        public array $renewers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->principalType);
        } else {
            $writer->writeString($this->principalType);
        }
        if ($flexible) {
            $writer->writeCompactString($this->principalName);
        } else {
            $writer->writeString($this->principalName);
        }
        if ($version >= 3) {
            $writer->writeCompactString($this->tokenRequesterPrincipalType);
        }
        if ($version >= 3) {
            $writer->writeCompactString($this->tokenRequesterPrincipalName);
        }
        $writer->writeInt64($this->issueTimestamp);
        $writer->writeInt64($this->expiryTimestamp);
        $writer->writeInt64($this->maxTimestamp);
        if ($flexible) {
            $writer->writeCompactString($this->tokenId);
        } else {
            $writer->writeString($this->tokenId);
        }
        if ($flexible) {
            $writer->writeCompactBytes($this->hmac);
        } else {
            $writer->writeBytes($this->hmac);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->renewers));
        } else {
            $writer->writeArrayLength(\count($this->renewers));
        }
        foreach ($this->renewers as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
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
        $issueTimestamp = $reader->readInt64();
        $expiryTimestamp = $reader->readInt64();
        $maxTimestamp = $reader->readInt64();
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
        if ($flexible) {
            $renewersCount = $reader->readCompactArrayLength();
        } else {
            $renewersCount = $reader->readArrayLength();
        }
        $renewers = [];
        for ($renewersIndex = 0; $renewersIndex < $renewersCount; ++$renewersIndex) {
            $item = DescribedDelegationToken\DescribedDelegationTokenRenewer::decode($reader, $version);
            $renewers[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($principalType, $principalName, $tokenRequesterPrincipalType, $tokenRequesterPrincipalName, $issueTimestamp, $expiryTimestamp, $maxTimestamp, $tokenId, $hmac, $renewers, $unknownTags);
    }
}
