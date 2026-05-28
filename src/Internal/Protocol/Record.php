<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Protocol;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @internal
 */
final readonly class Record
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
    public const int HEADER_SIZE = 61;

    /**
     * RecordBatch wire format version. Magic 0 and 1 are the legacy
     * MessageV0/V1 formats, we only produce and understand magic 2.
     */
    public const int MAGIC = 2;

    /**
     * @param list<Record\Header> $headers
     */
    public function __construct(
        public int $timestampDelta,
        public int $offsetDelta,
        public ?string $key,
        public ?string $value,
        public array $headers = [],
        public int $attributes = 0,
    ) {}

    public function encode(Writer $writer): void
    {
        $body = new Writer();
        $body->writeInt8($this->attributes);
        $body->writeVarint($this->timestampDelta);
        $body->writeVarint($this->offsetDelta);
        $body->writeVarintBytes($this->key);
        $body->writeVarintBytes($this->value);
        $body->writeVarint(\count($this->headers));

        foreach ($this->headers as $header) {
            $header->encode($body);
        }

        $bodyBytes = (string) $body;

        $writer->writeVarint(\strlen($bodyBytes));
        $writer->write($bodyBytes);
    }

    public static function decode(Reader $reader): static
    {
        $length = $reader->readVarint();
        $body = new Reader($reader->readBytesLength($length));

        $attributes = $body->readInt8();
        $timestampDelta = $body->readVarint();
        $offsetDelta = $body->readVarint();
        $key = $body->readVarintBytes();
        $value = $body->readVarintBytes();
        $headersCount = $body->readVarint();
        $headers = [];

        for ($index = 0; $index < $headersCount; ++$index) {
            $headers[] = Record\Header::decode($body);
        }

        return new self(
            timestampDelta: $timestampDelta,
            offsetDelta: $offsetDelta,
            key: $key,
            value: $value,
            headers: $headers,
            attributes: $attributes,
        );
    }
}
