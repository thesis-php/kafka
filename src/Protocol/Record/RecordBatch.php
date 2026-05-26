<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Record;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Crc32;
use Thesis\Kafka\Exception\CompressionFailed;
use Thesis\Kafka\Producer;
use Thesis\Kafka\Protocol\Writer;

/**
 * Accumulates records and serializes them as one magic=2 RecordBatch.
 *
 * RecordBatch is the modern message container (KIP-98):
 * a single header carries idempotency / transaction metadata, and the
 * records section holds variable-length, varint-encoded records
 * (optionally compressed as a whole).
 *
 * The wire layout the encode() method produces:
 *
 *     firstOffset           int64    base offset of the batch
 *     length                int32    bytes of everything that follows
 *     partitionLeaderEpoch  int32
 *     magic                 int8     always 2 for this class
 *     crc                   int32    CRC32C of everything after this field
 *     attributes            int16    compression codec, timestamp type, txn flags
 *     lastOffsetDelta       int32    numRecords - 1
 *     firstTimestamp        int64
 *     maxTimestamp          int64
 *     producerId            int64    -1 if not idempotent / transactional
 *     producerEpoch         int16    -1 if not idempotent / transactional
 *     baseSequence          int32    0 if not idempotent / transactional
 *     numRecords            int32
 *     records               bytes    optionally compressed; format see Record
 *
 * Not coroutine-safe — one builder per batch.
 *
 * @see https://kafka.apache.org/22/implementation/message-format/
 * @internal
 */
final class RecordBatch implements \Countable
{
    /**
     * Size in bytes of the fixed batch header. Counted from the
     * attributes field through numRecords, excluding the records
     * section itself. Used as the initial value for the on-wire size
     * estimate.
     *
     *   firstOffset          (int64)  8
     *   length               (int32)  4
     *   partitionLeaderEpoch (int32)  4
     *   magic                (int8)   1
     *   crc                  (int32)  4
     *   attributes           (int16)  2
     *   lastOffsetDelta      (int32)  4
     *   firstTimestamp       (int64)  8
     *   maxTimestamp         (int64)  8
     *   producerId           (int64)  8
     *   producerEpoch        (int16)  2
     *   baseSequence         (int32)  4
     *   numRecords           (int32)  4
     *                              = 61 bytes
     */
    private const int HEADER_SIZE = 61;

    /**
     * RecordBatch wire format version. Magic 0 and 1 are the legacy
     * MessageV0/V1 formats, we only produce and understand magic 2.
     */
    private const int MAGIC = 2;

    private readonly RecordBatchAttributes $attributes;

    /** @var list<Record> */
    private array $records = [];

    /**
     * Timestamp of the first record appended to the batch. All record
     * timestamps are stored as deltas off this value, so it has to be
     * captured once and kept stable.
     *
     * @var ?non-negative-int
     */
    private ?int $firstTimestamp = null;

    /**
     * Highest timestamp across all appended records.
     *
     * @var non-negative-int
     */
    private int $maxTimestamp = 0;

    /**
     * Updated in append() and read by the producer's Accumulator to decide
     * when to roll over to a new batch.
     *
     * @var positive-int
     */
    private int $estimatedSize = self::HEADER_SIZE;

    public function __construct(
        ?RecordBatchAttributes $attributes = null,
        private readonly ?Compressor $compressor = null,
        private readonly Crc32 $crc32c = new Crc32\Native(),
        private readonly int $producerId = -1,
        private readonly int $producerEpoch = -1,
        private readonly int $baseSequence = 0,
        private readonly int $partitionLeaderEpoch = -1,
        private readonly int $firstOffset = 0,
    ) {
        $this->attributes = $attributes ?? new RecordBatchAttributes(
            compression: $this->compressor?->codec() ?? Compressor\Codec::None,
        );

        $codec = $this->attributes->compression;

        if ($codec === Compressor\Codec::None && $this->compressor !== null) {
            throw new \LogicException('RecordBatch with Codec::None must not have a compressor.');
        }

        if ($codec !== Compressor\Codec::None && $this->compressor === null) {
            throw new \LogicException("RecordBatch with codec {$codec->name} requires a compressor.");
        }

        if ($this->compressor !== null && $this->compressor->codec() !== $codec) {
            throw new \LogicException(
                "Compressor codec {$this->compressor->codec()->name} does not match attributes codec {$codec->name}.",
            );
        }
    }

    public function append(Producer\Record $record): void
    {
        /** @var non-negative-int $timestamp */
        $timestamp = $record->timestamp ?? (int) floor(microtime(true) * 1000);

        // The first record's timestamp becomes the batch baseline; every
        // subsequent record is encoded as a delta of it.
        $this->firstTimestamp ??= $timestamp;
        $this->maxTimestamp = max($this->maxTimestamp, $timestamp);
        $this->estimatedSize += self::recordOverhead($record);

        $this->records[] = new Record(
            timestampDelta: $timestamp - $this->firstTimestamp,
            offsetDelta: \count($this->records),
            key: $record->key,
            value: $record->value,
            headers: array_map(
                Header::fromRecordHeader(...),
                $record->headers,
            ),
        );
    }

    /**
     * @return non-empty-string
     * @throws CompressionFailed
     */
    public function encode(): string
    {
        \assert($this->records !== [], 'cannot encode an empty record batch');

        $records = new Writer();
        foreach ($this->records as $record) {
            $record->encode($records);
        }

        /** @var non-empty-string $recordsBytes */
        $recordsBytes = (string) $records;
        if ($this->compressor !== null) {
            $recordsBytes = $this->compressor->compress($recordsBytes);
        }

        $crc = new Writer();
        $crc->writeInt16($this->attributes->toInt());
        $crc->writeInt32(\count($this->records) - 1);

        // firstTimestamp is non-null here because $records is non-empty-list
        // and append() always sets it on the first call. The ?? 0 is
        // just to satisfy the static analysis.
        $crc->writeInt64($this->firstTimestamp ?? 0);
        $crc->writeInt64($this->maxTimestamp);
        $crc->writeInt64($this->producerId);
        $crc->writeInt16($this->producerEpoch);
        $crc->writeInt32($this->baseSequence);
        $crc->writeInt32(\count($this->records));
        $crc->write($recordsBytes);

        /** @var non-empty-string $crcBuffer */
        $crcBuffer = (string) $crc;

        // The wire length field covers everything that follows it:
        //    partitionLeaderEpoch + magic + crc + the CRC payload itself.
        $length = 4 + 1 + 4 + \strlen($crcBuffer);

        $batch = new Writer();

        // firstOffset is conventionally 0 on the producer side,
        // and the broker rewrites it to the absolute log offset before persisting.
        $batch->writeInt64($this->firstOffset);
        $batch->writeInt32($length);
        $batch->writeInt32($this->partitionLeaderEpoch);
        $batch->writeInt8(self::MAGIC);
        $batch->writeInt32($this->crc32c->compute($crcBuffer));
        $batch->write($crcBuffer);

        /** @var non-empty-string */
        return (string) $batch;
    }

    /**
     * @return positive-int
     */
    public function estimatedSize(): int
    {
        return $this->estimatedSize;
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->records);
    }

    /**
     * @return positive-int
     */
    public static function recordOverhead(Producer\Record $record): int
    {
        // attributes + timestampDelta + offsetDelta
        $size = 1 + 10 + 5;
        $size += 5 + \strlen($record->key ?? '');
        $size += 5 + \strlen($record->value ?? '');
        $size += 5;

        foreach ($record->headers as $header) {
            $size += 5 + \strlen($header->key);
            $size += 5 + \strlen($header->value ?? '');
        }

        return $size + 5;
    }
}
