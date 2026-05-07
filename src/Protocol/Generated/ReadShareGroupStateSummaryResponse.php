<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ReadShareGroupStateSummaryResponse implements Response
{
    /**
     * @param list<ReadShareGroupStateSummaryResponse\ReadStateSummaryResult> $results The read results.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $results,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $resultsCount = $reader->readCompactArrayLength();
        $results = [];
        for ($resultsIndex = 0; $resultsIndex < $resultsCount; ++$resultsIndex) {
            $item = ReadShareGroupStateSummaryResponse\ReadStateSummaryResult::decode($reader, $version);
            $results[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($results, $unknownTags);
    }
}
