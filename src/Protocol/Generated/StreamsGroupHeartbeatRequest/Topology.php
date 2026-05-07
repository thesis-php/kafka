<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupHeartbeatRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Topology implements Encodable, Decodable
{
    /**
     * @param int $epoch The epoch of the topology. Used to check if the topology corresponds to the topology initialized on the brokers.
     * @param list<Topology\Subtopology> $subtopologies The sub-topologies of the streams application.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $epoch,
        public array $subtopologies,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->epoch);
        $writer->writeCompactArrayLength(\count($this->subtopologies));
        foreach ($this->subtopologies as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $epoch = $reader->readInt32();
        $subtopologiesCount = $reader->readCompactArrayLength();
        $subtopologies = [];
        for ($subtopologiesIndex = 0; $subtopologiesIndex < $subtopologiesCount; ++$subtopologiesIndex) {
            $item = Topology\Subtopology::decode($reader, $version);
            $subtopologies[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($epoch, $subtopologies, $unknownTags);
    }
}
