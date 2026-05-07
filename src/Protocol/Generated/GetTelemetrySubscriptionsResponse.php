<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class GetTelemetrySubscriptionsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param string $clientInstanceId Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
     * @param int $subscriptionId Unique identifier for the current subscription set for this client instance.
     * @param list<int> $acceptedCompressionTypes Compression types that broker accepts for the PushTelemetryRequest.
     * @param int $pushIntervalMs Configured push interval, which is the lowest configured interval in the current subscription set.
     * @param int $telemetryMaxBytes The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
     * @param bool $deltaTemporality Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
     * @param list<string> $requestedMetrics Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public string $clientInstanceId,
        public int $subscriptionId,
        public array $acceptedCompressionTypes,
        public int $pushIntervalMs,
        public int $telemetryMaxBytes,
        public bool $deltaTemporality,
        public array $requestedMetrics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $clientInstanceId = $reader->readUuid();
        $subscriptionId = $reader->readInt32();
        $acceptedCompressionTypesCount = $reader->readCompactArrayLength();
        $acceptedCompressionTypes = [];
        for ($acceptedCompressionTypesIndex = 0; $acceptedCompressionTypesIndex < $acceptedCompressionTypesCount; ++$acceptedCompressionTypesIndex) {
            $item = $reader->readInt8();
            $acceptedCompressionTypes[] = $item;
        }
        $pushIntervalMs = $reader->readInt32();
        $telemetryMaxBytes = $reader->readInt32();
        $deltaTemporality = $reader->readBool();
        $requestedMetricsCount = $reader->readCompactArrayLength();
        $requestedMetrics = [];
        for ($requestedMetricsIndex = 0; $requestedMetricsIndex < $requestedMetricsCount; ++$requestedMetricsIndex) {
            $item = $reader->readCompactString();
            $requestedMetrics[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $clientInstanceId, $subscriptionId, $acceptedCompressionTypes, $pushIntervalMs, $telemetryMaxBytes, $deltaTemporality, $requestedMetrics, $unknownTags);
    }
}
