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
final readonly class VotersRecord implements Encodable, Decodable
{
    /**
     * @param int $version The version of the voters record.
     * @param list<VotersRecord\Voter> $voters The set of voters in the quorum for this epoch.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $version,
        public array $voters,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt16($this->version);
        $writer->writeCompactArrayLength(\count($this->voters));
        foreach ($this->voters as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $version = $reader->readInt16();
        $votersCount = $reader->readCompactArrayLength();
        $voters = [];
        for ($votersIndex = 0; $votersIndex < $votersCount; ++$votersIndex) {
            $item = VotersRecord\Voter::decode($reader, $version);
            $voters[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($version, $voters, $unknownTags);
    }
}
