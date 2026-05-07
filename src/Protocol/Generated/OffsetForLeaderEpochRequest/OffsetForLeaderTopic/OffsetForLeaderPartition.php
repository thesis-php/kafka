<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\OffsetForLeaderEpochRequest\OffsetForLeaderTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class OffsetForLeaderPartition implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $currentLeaderEpoch An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
     * @param int $leaderEpoch The epoch to look up an offset for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $currentLeaderEpoch,
        public int $leaderEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        $writer->writeInt32($this->partition);
        if ($version >= 2) {
            $writer->writeInt32($this->currentLeaderEpoch);
        }
        $writer->writeInt32($this->leaderEpoch);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $partition = $reader->readInt32();
        $currentLeaderEpoch = -1;
        if ($version >= 2) {
            $currentLeaderEpoch = $reader->readInt32();
        }
        $leaderEpoch = $reader->readInt32();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partition, $currentLeaderEpoch, $leaderEpoch, $unknownTags);
    }
}
