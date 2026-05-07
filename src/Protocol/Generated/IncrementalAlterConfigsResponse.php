<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class IncrementalAlterConfigsResponse implements Response
{
    /**
     * @param int $throttleTimeMs Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<IncrementalAlterConfigsResponse\AlterConfigsResourceResponse> $responses The responses for each resource.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $responses,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $responsesCount = $reader->readCompactArrayLength();
        } else {
            $responsesCount = $reader->readArrayLength();
        }
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = IncrementalAlterConfigsResponse\AlterConfigsResourceResponse::decode($reader, $version);
            $responses[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $responses, $unknownTags);
    }
}
