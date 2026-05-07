<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ApiVersionsResponse implements Response
{
    /**
     * @param int $errorCode The top-level error code.
     * @param list<ApiVersionsResponse\ApiVersion> $apiKeys The APIs supported by the broker.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<ApiVersionsResponse\SupportedFeatureKey> $supportedFeatures Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
     * @param int $finalizedFeaturesEpoch The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
     * @param list<ApiVersionsResponse\FinalizedFeatureKey> $finalizedFeatures List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
     * @param bool $zkMigrationReady Set by a KRaft controller if the required configurations for ZK migration are present.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public array $apiKeys,
        public int $throttleTimeMs,
        public array $supportedFeatures,
        public int $finalizedFeaturesEpoch,
        public array $finalizedFeatures,
        public bool $zkMigrationReady,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $apiKeysCount = $reader->readCompactArrayLength();
        } else {
            $apiKeysCount = $reader->readArrayLength();
        }
        $apiKeys = [];
        for ($apiKeysIndex = 0; $apiKeysIndex < $apiKeysCount; ++$apiKeysIndex) {
            $item = ApiVersionsResponse\ApiVersion::decode($reader, $version);
            $apiKeys[] = $item;
        }
        $throttleTimeMs = 0;
        if ($version >= 1) {
            $throttleTimeMs = $reader->readInt32();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $supportedFeaturesCount = $tagReader->readCompactArrayLength();
                $supportedFeatures = [];
                for ($supportedFeaturesIndex = 0; $supportedFeaturesIndex < $supportedFeaturesCount; ++$supportedFeaturesIndex) {
                    $item = ApiVersionsResponse\SupportedFeatureKey::decode($tagReader, $version);
                    $supportedFeatures[] = $item;
                }
                unset($unknownTags[0]);
            } else {
                $supportedFeatures = [];
            }
            if (isset($unknownTags[1])) {
                $tagReader = new Reader($unknownTags[1]);
                $finalizedFeaturesEpoch = $tagReader->readInt64();
                unset($unknownTags[1]);
            } else {
                $finalizedFeaturesEpoch = -1;
            }
            if (isset($unknownTags[2])) {
                $tagReader = new Reader($unknownTags[2]);
                $finalizedFeaturesCount = $tagReader->readCompactArrayLength();
                $finalizedFeatures = [];
                for ($finalizedFeaturesIndex = 0; $finalizedFeaturesIndex < $finalizedFeaturesCount; ++$finalizedFeaturesIndex) {
                    $item = ApiVersionsResponse\FinalizedFeatureKey::decode($tagReader, $version);
                    $finalizedFeatures[] = $item;
                }
                unset($unknownTags[2]);
            } else {
                $finalizedFeatures = [];
            }
            if (isset($unknownTags[3])) {
                $tagReader = new Reader($unknownTags[3]);
                $zkMigrationReady = $tagReader->readBool();
                unset($unknownTags[3]);
            } else {
                $zkMigrationReady = false;
            }
        } else {
            $unknownTags = new UnknownTags();
            $supportedFeatures = [];
            $finalizedFeaturesEpoch = -1;
            $finalizedFeatures = [];
            $zkMigrationReady = false;
        }
        return new self($errorCode, $apiKeys, $throttleTimeMs, $supportedFeatures, $finalizedFeaturesEpoch, $finalizedFeatures, $zkMigrationReady, $unknownTags);
    }
}
