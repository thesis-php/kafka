<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Endian;
use Thesis\Kafka\Exception\UnexpectedEof;
use Thesis\Kafka\Exception\UnexpectedValue;
use Thesis\Varint;

/**
 * Reads Kafka wire-format primitives from an in-memory byte buffer.
 *
 * @api
 */
final class Reader
{
    /** @var non-negative-int */
    private int $offset = 0;

    /** @var non-negative-int */
    private readonly int $length;

    public function __construct(
        private readonly string $buffer,
    ) {
        $this->length = \strlen($this->buffer);
    }

    /**
     * Bytes left between the cursor and the end of the buffer.
     *
     * @return non-negative-int
     */
    public function remaining(): int
    {
        return max($this->length - $this->offset, 0);
    }

    /**
     * @param non-negative-int $skip
     */
    public function peekInt32(int $skip = 0): int
    {
        $value = $this->peek(4, $skip);

        return Endian\Order::Network->unpackInt32($value);
    }

    /**
     * @param non-negative-int $offset
     */
    public function reset(int $offset): void
    {
        $this->offset = $offset;
    }

    public function readInt8(): int
    {
        return Endian\Order::Network->unpackInt8($this->read(1));
    }

    public function readInt16(): int
    {
        return Endian\Order::Network->unpackInt16($this->read(2));
    }

    public function readInt32(): int
    {
        return Endian\Order::Network->unpackInt32($this->read(4));
    }

    /**
     * @return non-negative-int
     */
    public function readUint32(): int
    {
        return Endian\Order::Network->unpackUint32($this->read(4));
    }

    public function readInt64(): int
    {
        return (int) Endian\Order::Network->unpackInt64($this->read(8))->value;
    }

    /**
     * @return non-negative-int
     */
    public function readUint16(): int
    {
        return Endian\Order::Network->unpackUint16($this->read(2));
    }

    public function readFloat64(): float
    {
        return Endian\Order::Network->unpackDouble($this->read(8));
    }

    public function readBool(): bool
    {
        return $this->readInt8() !== 0;
    }

    /**
     * @return non-negative-int
     */
    public function readUnsignedVarint(): int
    {
        $buffer = substr($this->buffer, $this->offset);
        \assert($buffer !== '');

        $sized = Varint\BcMath::Codec->decodeVarintSized($buffer);
        $this->offset += $sized->size;

        /** @var non-negative-int */
        return (int) $sized->value->value;
    }

    public function readVarint(): int
    {
        $buffer = substr($this->buffer, $this->offset);
        \assert($buffer !== '');

        $sized = Varint\BcMath::Codec->decodeVarintSized($buffer);
        $this->offset += $sized->size;

        return (int) Varint\BcMath::Codec->decodeZigZag($sized->value)->value;
    }

    public function readString(): string
    {
        $length = $this->readInt16();
        if ($length < 0) {
            throw new UnexpectedValue(
                "Non-nullable string had negative length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readNullableString(): ?string
    {
        $length = $this->readInt16();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid nullable string length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readCompactString(): string
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            throw new UnexpectedValue(
                'Non-nullable compact string had null marker',
            );
        }

        --$length;

        return $length === 0 ? '' : $this->read($length);
    }

    public function readCompactNullableString(): ?string
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            return null;
        }

        --$length;

        return $length === 0 ? '' : $this->read($length);
    }

    public function readVarintString(): string
    {
        $length = $this->readVarint();
        if ($length < 0) {
            throw new UnexpectedValue(
                "Non-nullable varint string had negative length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readBytes(): string
    {
        $length = $this->readInt32();
        if ($length < 0) {
            throw new UnexpectedValue(
                "Non-nullable bytes had negative length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readNullableBytes(): ?string
    {
        $length = $this->readInt32();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid nullable bytes length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readCompactBytes(): string
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            throw new UnexpectedValue(
                'Non-nullable compact bytes had null marker',
            );
        }

        --$length;

        return $length === 0 ? '' : $this->read($length);
    }

    public function readCompactNullableBytes(): ?string
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            return null;
        }

        --$length;

        return $length === 0 ? '' : $this->read($length);
    }

    public function readVarintBytes(): ?string
    {
        $length = $this->readVarint();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid varint bytes length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readBytesLength(int $length): string
    {
        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid bytes length {$length}",
            );
        }

        return $length === 0 ? '' : $this->read($length);
    }

    public function readUuid(): UuidInterface
    {
        return Uuid::fromBytes($this->read(16));
    }

    /**
     * @return non-negative-int
     */
    public function readArrayLength(): int
    {
        $length = $this->readInt32();
        if ($length === -1) {
            throw new UnexpectedValue(
                'Non-nullable array had null marker',
            );
        }

        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid array length {$length}",
            );
        }

        return $length;
    }

    /**
     * @return ?non-negative-int
     */
    public function readNullableArrayLength(): ?int
    {
        $length = $this->readInt32();
        if ($length === -1) {
            return null;
        }

        if ($length < 0) {
            throw new UnexpectedValue(
                "Invalid nullable array length {$length}",
            );
        }

        return $length;
    }

    /**
     * @return non-negative-int
     */
    public function readCompactArrayLength(): int
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            throw new UnexpectedValue(
                'Non-nullable compact array had null marker',
            );
        }

        return $length - 1;
    }

    /**
     * @return ?non-negative-int
     */
    public function readCompactNullableArrayLength(): ?int
    {
        $length = $this->readUnsignedVarint();
        if ($length === 0) {
            return null;
        }

        return $length - 1;
    }

    public function readTaggedFields(): UnknownTags
    {
        $count = $this->readUnsignedVarint();
        $tags = new UnknownTags();

        for ($i = 0; $i < $count; ++$i) {
            $tag = $this->readUnsignedVarint();
            $size = $this->readUnsignedVarint();
            $tags[$tag] = $size === 0 ? '' : $this->read($size);
        }

        return $tags;
    }

    /**
     * @param non-negative-int $n
     * @return non-empty-string
     * @throws UnexpectedEof
     */
    public function read(int $n): string
    {
        if ($this->offset + $n > $this->length) {
            throw new UnexpectedEof(\sprintf(
                'Cannot read %d byte(s) at offset %d: only %d byte(s) remaining',
                $n,
                $this->offset,
                $this->length - $this->offset,
            ));
        }

        $chunk = substr($this->buffer, $this->offset, $n);
        $this->offset += $n;

        /** @var non-empty-string */
        return $chunk;
    }

    /**
     * @param positive-int $n
     * @param non-negative-int $skip
     * @return non-empty-string
     */
    public function peek(int $n, int $skip = 0): string
    {
        $at = $this->offset + $skip;

        if (($at + $n) > $this->length) {
            throw new \OutOfBoundsException(
                "Cannot peek {$n}: out of buffer bounds.",
            );
        }

        /** @var non-empty-string */
        return substr($this->buffer, $at, $n);
    }
}
