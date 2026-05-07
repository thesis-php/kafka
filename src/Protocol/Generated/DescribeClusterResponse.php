<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class DescribeClusterResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The top-level error message, or null if there was no error.
     * @param int $endpointType The endpoint type that was described. 1=brokers, 2=controllers.
     * @param string $clusterId The cluster ID that responding broker belongs to.
     * @param int $controllerId The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
     * @param list<DescribeClusterResponse\DescribeClusterBroker> $brokers Each broker in the response.
     * @param int $clusterAuthorizedOperations 32-bit bitfield to represent authorized operations for this cluster.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public int $endpointType,
        public string $clusterId,
        public int $controllerId,
        public array $brokers,
        public int $clusterAuthorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $endpointType = 1;
        if ($version >= 1) {
            $endpointType = $reader->readInt8();
        }
        $clusterId = $reader->readCompactString();
        $controllerId = $reader->readInt32();
        $brokersCount = $reader->readCompactArrayLength();
        $brokers = [];
        for ($brokersIndex = 0; $brokersIndex < $brokersCount; ++$brokersIndex) {
            $item = DescribeClusterResponse\DescribeClusterBroker::decode($reader, $version);
            $brokers[] = $item;
        }
        $clusterAuthorizedOperations = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $errorMessage, $endpointType, $clusterId, $controllerId, $brokers, $clusterAuthorizedOperations, $unknownTags);
    }
}
