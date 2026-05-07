<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class UpdateFeaturesResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top-level error code, or `0` if there was no top-level error.
     * @param ?string $errorMessage The top-level error message, or `null` if there was no top-level error.
     * @param list<UpdateFeaturesResponse\UpdatableFeatureResult> $results Results for each feature update.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public array $results,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $results = [];
        if ($version <= 1) {
            $resultsCount = $reader->readCompactArrayLength();
            $results = [];
            for ($resultsIndex = 0; $resultsIndex < $resultsCount; ++$resultsIndex) {
                $item = UpdateFeaturesResponse\UpdatableFeatureResult::decode($reader, $version);
                $results[] = $item;
            }
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $errorMessage, $results, $unknownTags);
    }
}
