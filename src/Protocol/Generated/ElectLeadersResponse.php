<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class ElectLeadersResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param list<ElectLeadersResponse\ReplicaElectionResult> $replicaElectionResults The election results, or an empty array if the requester did not have permission and the request asks for all partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $replicaElectionResults,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = 0;
        if ($version >= 1) {
            $errorCode = $reader->readInt16();
        }
        if ($flexible) {
            $replicaElectionResultsCount = $reader->readCompactArrayLength();
        } else {
            $replicaElectionResultsCount = $reader->readArrayLength();
        }
        $replicaElectionResults = [];
        for ($replicaElectionResultsIndex = 0; $replicaElectionResultsIndex < $replicaElectionResultsCount; ++$replicaElectionResultsIndex) {
            $item = ElectLeadersResponse\ReplicaElectionResult::decode($reader, $version);
            $replicaElectionResults[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $replicaElectionResults, $unknownTags);
    }
}
