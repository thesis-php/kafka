<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreatePartitionsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatePartitionsTopic implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param int $count The new partition count.
     * @param ?list<CreatePartitionsTopic\CreatePartitionsAssignment> $assignments The new partition assignments.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $count,
        public ?array $assignments,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        $writer->writeInt32($this->count);
        if ($flexible) {
            $writer->writeCompactNullableArrayLength($this->assignments === null ? null : \count($this->assignments));
        } else {
            $writer->writeNullableArrayLength($this->assignments === null ? null : \count($this->assignments));
        }
        if ($this->assignments !== null) {
            foreach ($this->assignments as $item) {
                $item->encode($writer, $version);
            }
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
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $count = $reader->readInt32();
        if ($flexible) {
            $assignmentsCount = $reader->readCompactNullableArrayLength();
        } else {
            $assignmentsCount = $reader->readNullableArrayLength();
        }
        if ($assignmentsCount === null) {
            $assignments = null;
        } else {
            $assignments = [];
            for ($assignmentsIndex = 0; $assignmentsIndex < $assignmentsCount; ++$assignmentsIndex) {
                $item = CreatePartitionsTopic\CreatePartitionsAssignment::decode($reader, $version);
                $assignments[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $count, $assignments, $unknownTags);
    }
}
