<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchResponse\FetchableTopicResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionData implements Encodable, Decodable
{
    /**
     * @param int $partitionIndex The partition index.
     * @param int $errorCode The error code, or 0 if there was no fetch error.
     * @param int $highWatermark The current high water mark.
     * @param int $lastStableOffset The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
     * @param int $logStartOffset The current log start offset.
     * @param PartitionData\EpochEndOffset $divergingEpoch In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
     * @param PartitionData\LeaderIdAndEpoch $currentLeader The current leader of the partition.
     * @param PartitionData\SnapshotId $snapshotId In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
     * @param ?list<PartitionData\AbortedTransaction> $abortedTransactions The aborted transactions.
     * @param int $preferredReadReplica The preferred read replica for the consumer to use on its next fetch request.
     * @param ?string $records The record data.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partitionIndex,
        public int $errorCode,
        public int $highWatermark,
        public int $lastStableOffset,
        public int $logStartOffset,
        public PartitionData\EpochEndOffset $divergingEpoch,
        public PartitionData\LeaderIdAndEpoch $currentLeader,
        public PartitionData\SnapshotId $snapshotId,
        public ?array $abortedTransactions,
        public int $preferredReadReplica,
        public ?string $records,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt32($this->partitionIndex);
        $writer->writeInt16($this->errorCode);
        $writer->writeInt64($this->highWatermark);
        if ($version >= 4) {
            $writer->writeInt64($this->lastStableOffset);
        }
        if ($version >= 5) {
            $writer->writeInt64($this->logStartOffset);
        }
        if ($version >= 4) {
            if ($flexible) {
                $writer->writeCompactNullableArrayLength($this->abortedTransactions === null ? null : \count($this->abortedTransactions));
            } else {
                $writer->writeNullableArrayLength($this->abortedTransactions === null ? null : \count($this->abortedTransactions));
            }
            if ($this->abortedTransactions !== null) {
                foreach ($this->abortedTransactions as $item) {
                    $item->encode($writer, $version);
                }
            }
        }
        if ($version >= 11) {
            $writer->writeInt32($this->preferredReadReplica);
        }
        if ($flexible) {
            $writer->writeCompactNullableBytes($this->records);
        } else {
            $writer->writeNullableBytes($this->records);
        }
        if ($flexible) {
            $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
            if (isset($taggedFields[0])) {
                throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'DivergingEpoch\'.');
            }
            $tagWriter = new Writer();
            $this->divergingEpoch->encode($tagWriter, $version);
            $taggedFields[0] = (string) $tagWriter;
            if (isset($taggedFields[1])) {
                throw new \UnexpectedValueException('Unknown tag \'1\' collides with known field \'CurrentLeader\'.');
            }
            $tagWriter = new Writer();
            $this->currentLeader->encode($tagWriter, $version);
            $taggedFields[1] = (string) $tagWriter;
            if (isset($taggedFields[2])) {
                throw new \UnexpectedValueException('Unknown tag \'2\' collides with known field \'SnapshotId\'.');
            }
            $tagWriter = new Writer();
            $this->snapshotId->encode($tagWriter, $version);
            $taggedFields[2] = (string) $tagWriter;
            $writer->writeTaggedFields($taggedFields);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $partitionIndex = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $highWatermark = $reader->readInt64();
        $lastStableOffset = -1;
        if ($version >= 4) {
            $lastStableOffset = $reader->readInt64();
        }
        $logStartOffset = -1;
        if ($version >= 5) {
            $logStartOffset = $reader->readInt64();
        }
        $abortedTransactions = null;
        if ($version >= 4) {
            if ($flexible) {
                $abortedTransactionsCount = $reader->readCompactNullableArrayLength();
            } else {
                $abortedTransactionsCount = $reader->readNullableArrayLength();
            }
            if ($abortedTransactionsCount === null) {
                $abortedTransactions = null;
            } else {
                $abortedTransactions = [];
                for ($abortedTransactionsIndex = 0; $abortedTransactionsIndex < $abortedTransactionsCount; ++$abortedTransactionsIndex) {
                    $item = PartitionData\AbortedTransaction::decode($reader, $version);
                    $abortedTransactions[] = $item;
                }
            }
        }
        $preferredReadReplica = -1;
        if ($version >= 11) {
            $preferredReadReplica = $reader->readInt32();
        }
        if ($flexible) {
            $records = $reader->readCompactNullableBytes();
        } else {
            $records = $reader->readNullableBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $divergingEpoch = PartitionData\EpochEndOffset::decode($tagReader, $version);
                unset($unknownTags[0]);
            } else {
                $divergingEpoch = new PartitionData\EpochEndOffset(-1, -1);
            }
            if (isset($unknownTags[1])) {
                $tagReader = new Reader($unknownTags[1]);
                $currentLeader = PartitionData\LeaderIdAndEpoch::decode($tagReader, $version);
                unset($unknownTags[1]);
            } else {
                $currentLeader = new PartitionData\LeaderIdAndEpoch(-1, -1);
            }
            if (isset($unknownTags[2])) {
                $tagReader = new Reader($unknownTags[2]);
                $snapshotId = PartitionData\SnapshotId::decode($tagReader, $version);
                unset($unknownTags[2]);
            } else {
                $snapshotId = new PartitionData\SnapshotId(-1, -1);
            }
        } else {
            $unknownTags = new UnknownTags();
            $divergingEpoch = new PartitionData\EpochEndOffset(-1, -1);
            $currentLeader = new PartitionData\LeaderIdAndEpoch(-1, -1);
            $snapshotId = new PartitionData\SnapshotId(-1, -1);
        }
        return new self($partitionIndex, $errorCode, $highWatermark, $lastStableOffset, $logStartOffset, $divergingEpoch, $currentLeader, $snapshotId, $abortedTransactions, $preferredReadReplica, $records, $unknownTags);
    }
}
