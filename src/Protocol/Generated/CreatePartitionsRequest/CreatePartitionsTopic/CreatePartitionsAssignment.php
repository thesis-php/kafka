<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreatePartitionsRequest\CreatePartitionsTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatePartitionsAssignment implements Encodable, Decodable
{
    /**
     * @param list<int> $brokerIds The assigned broker IDs.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $brokerIds,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->brokerIds));
        } else {
            $writer->writeArrayLength(\count($this->brokerIds));
        }
        foreach ($this->brokerIds as $item) {
            $writer->writeInt32($item);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $brokerIdsCount = $reader->readCompactArrayLength();
        } else {
            $brokerIdsCount = $reader->readArrayLength();
        }
        $brokerIds = [];
        for ($brokerIdsIndex = 0; $brokerIdsIndex < $brokerIdsCount; ++$brokerIdsIndex) {
            $item = $reader->readInt32();
            $brokerIds[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($brokerIds, $unknownTags);
    }
}
