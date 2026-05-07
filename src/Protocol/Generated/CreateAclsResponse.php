<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class CreateAclsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<CreateAclsResponse\AclCreationResult> $results The results for each ACL creation.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $results,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $throttleTimeMs = $reader->readInt32();
        if ($flexible) {
            $resultsCount = $reader->readCompactArrayLength();
        } else {
            $resultsCount = $reader->readArrayLength();
        }
        $results = [];
        for ($resultsIndex = 0; $resultsIndex < $resultsCount; ++$resultsIndex) {
            $item = CreateAclsResponse\AclCreationResult::decode($reader, $version);
            $results[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $results, $unknownTags);
    }
}
