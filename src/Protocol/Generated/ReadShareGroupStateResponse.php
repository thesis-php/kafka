<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ReadShareGroupStateResponse implements Response
{
    /**
     * @param list<ReadShareGroupStateResponse\ReadStateResult> $results The read results.
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
            $item = ReadShareGroupStateResponse\ReadStateResult::decode($reader, $version);
            $results[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($results, $unknownTags);
    }
}
