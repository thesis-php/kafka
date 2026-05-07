<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeClientQuotasResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or `0` if the quota description succeeded.
     * @param ?string $errorMessage The error message, or `null` if the quota description succeeded.
     * @param ?list<DescribeClientQuotasResponse\EntryData> $entries A result entry.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public ?array $entries,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $errorMessage = $reader->readCompactNullableString();
        } else {
            $errorMessage = $reader->readNullableString();
        }
        if ($flexible) {
            $entriesCount = $reader->readCompactNullableArrayLength();
        } else {
            $entriesCount = $reader->readNullableArrayLength();
        }
        if ($entriesCount === null) {
            $entries = null;
        } else {
            $entries = [];
            for ($entriesIndex = 0; $entriesIndex < $entriesCount; ++$entriesIndex) {
                $item = DescribeClientQuotasResponse\EntryData::decode($reader, $version);
                $entries[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $errorMessage, $entries, $unknownTags);
    }
}
