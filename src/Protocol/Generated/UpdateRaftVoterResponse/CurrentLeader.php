<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\UpdateRaftVoterResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CurrentLeader implements Encodable, Decodable
{
    /**
     * @param int $leaderId The replica id of the current leader or -1 if the leader is unknown.
     * @param int $leaderEpoch The latest known leader epoch.
     * @param string $host The node's hostname.
     * @param int $port The node's port.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $leaderId,
        public int $leaderEpoch,
        public string $host,
        public int $port,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->leaderId);
        $writer->writeInt32($this->leaderEpoch);
        $writer->writeCompactString($this->host);
        $writer->writeInt32($this->port);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $leaderId = $reader->readInt32();
        $leaderEpoch = $reader->readInt32();
        $host = $reader->readCompactString();
        $port = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($leaderId, $leaderEpoch, $host, $port, $unknownTags);
    }
}
