<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class UpdateRaftVoterResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param UpdateRaftVoterResponse\CurrentLeader $currentLeader Details of the current Raft cluster leader.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public UpdateRaftVoterResponse\CurrentLeader $currentLeader,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $unknownTags = $reader->readTaggedFields();
        if (isset($unknownTags[0])) {
            $tagReader = new Reader($unknownTags[0]);
            $currentLeader = UpdateRaftVoterResponse\CurrentLeader::decode($tagReader, $version);
            unset($unknownTags[0]);
        } else {
            $currentLeader = new UpdateRaftVoterResponse\CurrentLeader(-1, -1, '', 0);
        }
        return new self($throttleTimeMs, $errorCode, $currentLeader, $unknownTags);
    }
}
