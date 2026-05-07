<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeLogDirsResponse\DescribeLogDirsResult\DescribeLogDirsTopic;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeLogDirsPartition implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $partitionSize The size of the log segments in this partition in bytes.
     * @param int $offsetLag The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
     * @param bool $isFutureKey True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $partitionSize,
        public int $offsetLag,
        public bool $isFutureKey,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt64($this->partitionSize);
        $writer->writeInt64($this->offsetLag);
        $writer->writeBool($this->isFutureKey);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $partitionIndex = $reader->readInt32();
        $partitionSize = $reader->readInt64();
        $offsetLag = $reader->readInt64();
        $isFutureKey = $reader->readBool();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($partitionIndex, $partitionSize, $offsetLag, $isFutureKey, $unknownTags);
    }
}
