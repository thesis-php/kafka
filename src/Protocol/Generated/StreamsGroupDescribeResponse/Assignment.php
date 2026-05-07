<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\StreamsGroupDescribeResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Assignment implements Encodable, Decodable
{
    /**
     * @param list<TaskIds> $activeTasks Active tasks for this client.
     * @param list<TaskIds> $standbyTasks Standby tasks for this client.
     * @param list<TaskIds> $warmupTasks Warm-up tasks for this client.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $activeTasks,
        public array $standbyTasks,
        public array $warmupTasks,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->activeTasks));
        foreach ($this->activeTasks as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->standbyTasks));
        foreach ($this->standbyTasks as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->warmupTasks));
        foreach ($this->warmupTasks as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $activeTasksCount = $reader->readCompactArrayLength();
        $activeTasks = [];
        for ($activeTasksIndex = 0; $activeTasksIndex < $activeTasksCount; ++$activeTasksIndex) {
            $item = TaskIds::decode($reader, $version);
            $activeTasks[] = $item;
        }
        $standbyTasksCount = $reader->readCompactArrayLength();
        $standbyTasks = [];
        for ($standbyTasksIndex = 0; $standbyTasksIndex < $standbyTasksCount; ++$standbyTasksIndex) {
            $item = TaskIds::decode($reader, $version);
            $standbyTasks[] = $item;
        }
        $warmupTasksCount = $reader->readCompactArrayLength();
        $warmupTasks = [];
        for ($warmupTasksIndex = 0; $warmupTasksIndex < $warmupTasksCount; ++$warmupTasksIndex) {
            $item = TaskIds::decode($reader, $version);
            $warmupTasks[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($activeTasks, $standbyTasks, $warmupTasks, $unknownTags);
    }
}
