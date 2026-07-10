<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\Protocol;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Crc32;
use Thesis\Kafka\Exception\CompressionFailed;
use Thesis\Kafka\Exception\CorruptRecordBatch;
use Thesis\Kafka\Exception\UnexpectedEof;
use Thesis\Kafka\Exception\UnsupportedCodec;
use Thesis\Kafka\Exception\UnsupportedMessageFormat;
use Thesis\Kafka\Internal\Protocol\Record;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\RecordHeader;

/**
 * @internal
 */
final readonly class RecordBatch
{
    public function __construct(
        private Compressor\Registry $decompressors,
        private Crc32 $crc32c,
    ) {}

    /**
     * @param non-empty-string $blob
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @return list<Records>
     * @throws CorruptRecordBatch a batch's CRC didn't match
     * @throws UnsupportedMessageFormat a batch used magic other than 2
     * @throws UnsupportedCodec a batch's codec has no decompressor
     * @throws UnexpectedEof
     * @throws CompressionFailed
     */
    public function decode(
        string $blob,
        string $topic,
        int $partition,
    ): array {
        $reader = new Reader($blob);
        $records = [];

        // Walk batch by batch. Each batch announces its own length, so
        // we know exactly how many bytes it occupies before reading it.
        while ($reader->remaining() >= Record::HEADER_SIZE) {
            // The length field sits right after the 8-byte firstOffset
            // and covers everything after itself. The whole batch is
            // therefore firstOffset(8) + lengthField(4) + length bytes.
            $length = $reader->peekInt32(8);

            /** @var positive-int $batchSize */
            $batchSize = 8 + 4 + $length;

            if ($reader->remaining() < $batchSize) {
                // Truncated trailing batch — drop it. The broker re-sends
                // this batch whole when we next fetch from its offset.
                break;
            }

            $records[] = $this->decodeRecords(
                $reader->read($batchSize),
                $topic,
                $partition,
            );
        }

        return $records;
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @throws CorruptRecordBatch
     * @throws UnsupportedMessageFormat
     * @throws UnsupportedCodec
     * @throws UnexpectedEof
     * @throws CompressionFailed
     */
    private function decodeRecords(
        string $bytes,
        string $topic,
        int $partition,
    ): Records {
        $reader = new Reader($bytes);

        $firstOffset = $reader->readInt64();
        $length = $reader->readInt32();
        $partitionLeaderEpoch = $reader->readInt32();

        $magic = $reader->readInt8();
        if ($magic !== Record::MAGIC) {
            throw new UnsupportedMessageFormat("Invalid magic {$magic}, expected: 2.");
        }

        $expectedCrc = $reader->readInt32();

        $remaining = $reader->remaining();
        \assert($remaining > 0);

        // The CRC covers every byte after the crc field itself. Verify
        // before trusting attributes, counts, or payload.
        $crcPayload = $reader->peek($remaining);

        $actualCrc = $this->crc32c->compute($crcPayload);
        if ($actualCrc !== $expectedCrc) {
            throw new CorruptRecordBatch("Batch crc32 mismatch: expected {$expectedCrc}, got {$actualCrc}.");
        }

        $attributes = Record\Attributes::fromInt($reader->readInt16());
        $lastOffsetDelta = $reader->readInt32();
        $firstTimestamp = $reader->readInt64();

        $maxTimestamp = $reader->readInt64();
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt16();
        $baseSequence = $reader->readInt32();
        $recordCount = $reader->readInt32();

        $records = new Records(
            topic: $topic,
            partition: $partition,
            firstOffset: $firstOffset,
            length: $length,
            partitionLeaderEpoch: $partitionLeaderEpoch,
            attributes: $attributes,
            lastOffsetDelta: $lastOffsetDelta,
            firstTimestamp: $firstTimestamp,
            maxTimestamp: $maxTimestamp,
            producerId: $producerId,
            producerEpoch: $producerEpoch,
            baseSequence: $baseSequence,
        );

        if (!$attributes->isControlBatch && $recordCount > 0) {
            $bytes = $reader->read($reader->remaining());

            $decompressor = $this->decompressors->for($attributes->compression);
            $bytes = $decompressor->decompress($bytes);

            $reader = new Reader($bytes);

            for ($i = 0; $i < $recordCount; ++$i) {
                $record = Record::decode($reader);

                $records->add(new Consumer\Record(
                    topic: $records->topic,
                    partition: $records->partition,
                    offset: max($records->firstOffset + $record->offsetDelta, 0),
                    key: $record->key,
                    value: $record->value,
                    headers: array_map(
                        static fn(Record\Header $header) => new RecordHeader(
                            key: $header->key,
                            value: $header->value,
                        ),
                        $record->headers,
                    ),
                    timestamp: $records->firstTimestamp + $record->timestampDelta,
                    leaderEpoch: $records->partitionLeaderEpoch,
                    producerId: $records->producerId,
                    producerEpoch: $records->producerEpoch,
                ));
            }
        }

        return $records;
    }
}
