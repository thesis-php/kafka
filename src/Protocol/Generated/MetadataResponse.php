<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class MetadataResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<MetadataResponse\MetadataResponseBroker> $brokers A list of brokers present in the cluster.
     * @param ?string $clusterId The cluster ID that responding broker belongs to.
     * @param int $controllerId The ID of the controller broker.
     * @param list<MetadataResponse\MetadataResponseTopic> $topics Each topic in the response.
     * @param int $clusterAuthorizedOperations 32-bit bitfield to represent authorized operations for this cluster.
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public array $brokers,
        public ?string $clusterId,
        public int $controllerId,
        public array $topics,
        public int $clusterAuthorizedOperations,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $throttleTimeMs = 0;
        if ($version >= 3) {
            $throttleTimeMs = $reader->readInt32();
        }
        if ($flexible) {
            $brokersCount = $reader->readCompactArrayLength();
        } else {
            $brokersCount = $reader->readArrayLength();
        }
        $brokers = [];
        for ($brokersIndex = 0; $brokersIndex < $brokersCount; ++$brokersIndex) {
            $item = MetadataResponse\MetadataResponseBroker::decode($reader, $version);
            $brokers[] = $item;
        }
        $clusterId = null;
        if ($version >= 2) {
            if ($flexible) {
                $clusterId = $reader->readCompactNullableString();
            } else {
                $clusterId = $reader->readNullableString();
            }
        }
        $controllerId = -1;
        if ($version >= 1) {
            $controllerId = $reader->readInt32();
        }
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = MetadataResponse\MetadataResponseTopic::decode($reader, $version);
            $topics[] = $item;
        }
        $clusterAuthorizedOperations = -2147483648;
        if ($version >= 8 && $version <= 10) {
            $clusterAuthorizedOperations = $reader->readInt32();
        }
        $errorCode = 0;
        if ($version >= 13) {
            $errorCode = $reader->readInt16();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $brokers, $clusterId, $controllerId, $topics, $clusterAuthorizedOperations, $errorCode, $unknownTags);
    }
}
