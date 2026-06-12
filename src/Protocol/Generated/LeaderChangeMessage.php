<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class LeaderChangeMessage implements Encodable, Decodable
{
    /**
     * @param int $version The version of the leader change message.
     * @param int $leaderId The ID of the newly elected leader.
     * @param list<LeaderChangeMessage\Voter> $voters The set of voters in the quorum for this epoch.
     * @param list<LeaderChangeMessage\Voter> $grantingVoters The voters who voted for the leader at the time of election.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $version,
        public int $leaderId,
        public array $voters,
        public array $grantingVoters,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->version);
        $writer->writeInt32($this->leaderId);
        $writer->writeCompactArrayLength(\count($this->voters));
        foreach ($this->voters as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->grantingVoters));
        foreach ($this->grantingVoters as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $version = $reader->readInt16();
        $leaderId = $reader->readInt32();
        $votersCount = $reader->readCompactArrayLength();
        $voters = [];
        for ($votersIndex = 0; $votersIndex < $votersCount; ++$votersIndex) {
            $item = LeaderChangeMessage\Voter::decode($reader, $version);
            $voters[] = $item;
        }
        $grantingVotersCount = $reader->readCompactArrayLength();
        $grantingVoters = [];
        for ($grantingVotersIndex = 0; $grantingVotersIndex < $grantingVotersCount; ++$grantingVotersIndex) {
            $item = LeaderChangeMessage\Voter::decode($reader, $version);
            $grantingVoters[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($version, $leaderId, $voters, $grantingVoters, $unknownTags);
    }
}
