<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AssignReplicasToDirsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param list<AssignReplicasToDirsResponse\DirectoryData> $directories The list of directories and their assigned partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $directories,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $directoriesCount = $reader->readCompactArrayLength();
        $directories = [];
        for ($directoriesIndex = 0; $directoriesIndex < $directoriesCount; ++$directoriesIndex) {
            $item = AssignReplicasToDirsResponse\DirectoryData::decode($reader, $version);
            $directories[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $directories, $unknownTags);
    }
}
