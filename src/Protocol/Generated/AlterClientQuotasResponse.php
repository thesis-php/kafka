<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AlterClientQuotasResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<AlterClientQuotasResponse\EntryData> $entries The quota configuration entries to alter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $entries,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $entriesCount = $reader->readCompactArrayLength();
        } else {
            $entriesCount = $reader->readArrayLength();
        }
        $entries = [];
        for ($entriesIndex = 0; $entriesIndex < $entriesCount; ++$entriesIndex) {
            $item = AlterClientQuotasResponse\EntryData::decode($reader, $version);
            $entries[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $entries, $unknownTags);
    }
}
