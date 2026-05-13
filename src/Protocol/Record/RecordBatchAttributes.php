<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Record;

use Thesis\Kafka\Protocol\Compressor\Codec;

/**
 * @internal
 */
final readonly class RecordBatchAttributes
{
    public function __construct(
        public Codec $compression = Codec::None,
        public TimestampType $timestampType = TimestampType::CreateTime,
        public bool $isTransactional = false,
        public bool $isControlBatch = false,
    ) {}

    public static function fromInt(int $value): self
    {
        $compression = Codec::from($value & 0b111);
        $timestampType = (($value >> 3) & 0b1) === 1
            ? TimestampType::LogAppendTime
            : TimestampType::CreateTime;

        return new self(
            compression: $compression,
            timestampType: $timestampType,
            isTransactional: (($value >> 4) & 0b1) === 1,
            isControlBatch: (($value >> 5) & 0b1) === 1,
        );
    }

    public function toInt(): int
    {
        return $this->compression->value
            | ($this->timestampType->value << 3)
            | ((int) $this->isTransactional << 4)
            | ((int) $this->isControlBatch << 5);
    }
}
