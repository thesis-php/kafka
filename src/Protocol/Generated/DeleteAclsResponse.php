<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DeleteAclsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<DeleteAclsResponse\DeleteAclsFilterResult> $filterResults The results for each filter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $filterResults,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $filterResultsCount = $reader->readCompactArrayLength();
        } else {
            $filterResultsCount = $reader->readArrayLength();
        }
        $filterResults = [];
        for ($filterResultsIndex = 0; $filterResultsIndex < $filterResultsCount; ++$filterResultsIndex) {
            $item = DeleteAclsResponse\DeleteAclsFilterResult::decode($reader, $version);
            $filterResults[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $filterResults, $unknownTags);
    }
}
