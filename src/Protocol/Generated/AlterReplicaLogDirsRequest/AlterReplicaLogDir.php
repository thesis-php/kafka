<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterReplicaLogDirsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AlterReplicaLogDir implements Encodable, Decodable
{
    /**
     * @param string $path The absolute directory path.
     * @param list<AlterReplicaLogDir\AlterReplicaLogDirTopic> $topics The topics to add to the directory.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $path,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->path);
        } else {
            $writer->writeString($this->path);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
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
            $path = $reader->readCompactString();
        } else {
            $path = $reader->readString();
        }
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = AlterReplicaLogDir\AlterReplicaLogDirTopic::decode($reader, $version);
            $topics[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($path, $topics, $unknownTags);
    }
}
