<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use BcMath\Number;
use Thesis\Endian;
use Thesis\Varint;

/**
 * Writes Kafka wire-format primitives into an in-memory byte buffer.
 *
 * @api
 */
final class Writer implements
    \Countable,
    \Stringable
{
    public function __construct(
        private string $buffer = '',
    ) {}

    public function writeInt8(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packInt8($value);
    }

    public function writeInt16(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packInt16($value);
    }

    public function writeInt32(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packInt32($value);
    }

    public function writeUint32(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packUint32($value);
    }

    public function writeInt64(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packInt64(new Number($value));
    }

    public function writeUint16(int $value): void
    {
        $this->buffer .= Endian\Order::Network->packUint16($value);
    }

    public function writeFloat64(float $value): void
    {
        $this->buffer .= Endian\Order::Network->packDouble($value);
    }

    public function writeBool(bool $value): void
    {
        $this->writeInt8($value ? 1 : 0);
    }

    public function writeUnsignedVarint(int $value): void
    {
        $this->buffer .= Varint\BcMath::Codec->encodeVarint(new Number($value));
    }

    public function writeString(string $value): void
    {
        $length = \strlen($value);
        $this->writeInt16($length);

        if ($length > 0) {
            $this->buffer .= $value;
        }
    }

    public function writeNullableString(?string $value): void
    {
        if ($value === null) {
            $this->writeInt16(-1);

            return;
        }

        $this->writeString($value);
    }

    public function writeCompactString(string $value): void
    {
        $length = \strlen($value);
        $this->writeUnsignedVarint($length + 1);

        if ($length > 0) {
            $this->buffer .= $value;
        }
    }

    public function writeCompactNullableString(?string $value): void
    {
        if ($value === null) {
            $this->writeUnsignedVarint(0);

            return;
        }

        $this->writeCompactString($value);
    }

    public function writeBytes(string $value): void
    {
        $length = \strlen($value);
        $this->writeInt32($length);

        if ($length > 0) {
            $this->buffer .= $value;
        }
    }

    public function writeNullableBytes(?string $value): void
    {
        if ($value === null) {
            $this->writeInt32(-1);

            return;
        }

        $this->writeBytes($value);
    }

    public function writeCompactBytes(string $value): void
    {
        $length = \strlen($value);
        $this->writeUnsignedVarint($length + 1);

        if ($length > 0) {
            $this->buffer .= $value;
        }
    }

    public function writeCompactNullableBytes(?string $value): void
    {
        if ($value === null) {
            $this->writeUnsignedVarint(0);

            return;
        }

        $this->writeCompactBytes($value);
    }

    public function writeUuid(string $value): void
    {
        if (\strlen($value) !== 16) {
            throw new \UnexpectedValueException(\sprintf(
                'UUID must be exactly 16 bytes, got %d',
                \strlen($value),
            ));
        }

        $this->buffer .= $value;
    }

    public function writeArrayLength(int $count): void
    {
        $this->writeInt32($count);
    }

    public function writeNullableArrayLength(?int $count): void
    {
        $this->writeInt32($count ?? -1);
    }

    public function writeCompactArrayLength(int $count): void
    {
        $this->writeUnsignedVarint($count + 1);
    }

    public function writeCompactNullableArrayLength(?int $count): void
    {
        $this->writeUnsignedVarint($count === null ? 0 : $count + 1);
    }

    public function writeTag(int $tag, string $value): void
    {
        $this->writeUnsignedVarint($tag);
        $this->writeUnsignedVarint(\strlen($value));

        if ($value !== '') {
            $this->buffer .= $value;
        }
    }

    public function writeTaggedFields(UnknownTags $tags): void
    {
        $this->writeUnsignedVarint(\count($tags));
        $tags->each($this->writeTag(...));
    }

    #[\Override]
    public function __toString(): string
    {
        return $this->buffer;
    }

    #[\Override]
    public function count(): int
    {
        return \strlen($this->buffer);
    }
}
