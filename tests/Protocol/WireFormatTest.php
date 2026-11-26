<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

use Testo\Assert;
use Testo\Data\DataProvider;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Exception\UnexpectedEof;
use Thesis\Kafka\Exception\UnexpectedValue;

/**
 * The expected bytes were written by the old encoder, which used BcMath.
 * They are equal to the bytes in ByteUtilsTest of the Kafka Java client.
 *
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/test/java/org/apache/kafka/common/utils/ByteUtilsTest.java
 */
final readonly class WireFormatTest
{
    private const int INT32_MAX = 2_147_483_647;
    private const int INT32_MIN = -2_147_483_648;

    /**
     * @return array<array-key, array{int, non-empty-string}>
     */
    public static function varints(): array
    {
        return [
            '0' => [0, '00'],
            '1' => [1, '02'],
            '-1' => [-1, '01'],
            '63' => [63, '7e'],
            '-64' => [-64, '7f'],
            '64' => [64, '8001'],
            '-65' => [-65, '8101'],
            '127' => [127, 'fe01'],
            '128' => [128, '8002'],
            'int32 max' => [self::INT32_MAX, 'feffffff0f'],
            'int32 min' => [self::INT32_MIN, 'ffffffff0f'],
            'int64 max' => [PHP_INT_MAX, 'feffffffffffffffff01'],
            'int64 min' => [PHP_INT_MIN, 'ffffffffffffffffff01'],
        ];
    }

    /**
     * @return array<array-key, array{int, non-empty-string}>
     */
    public static function unsignedVarints(): array
    {
        return [
            '0' => [0, '00'],
            '1' => [1, '01'],
            '127' => [127, '7f'],
            '128' => [128, '8001'],
            'int32 max' => [self::INT32_MAX, 'ffffffff07'],
            'int32 min as uint32' => [self::INT32_MAX + 1, '8080808008'],
            'uint32 max' => [2 * self::INT32_MAX + 1, 'ffffffff0f'],
            'int64 max' => [PHP_INT_MAX, 'ffffffffffffffff7f'],
        ];
    }

    /**
     * @return array<array-key, array{int, non-empty-string}>
     */
    public static function int64s(): array
    {
        return [
            '0' => [0, '0000000000000000'],
            '1' => [1, '0000000000000001'],
            '-1' => [-1, 'ffffffffffffffff'],
            '127' => [127, '000000000000007f'],
            '128' => [128, '0000000000000080'],
            'int32 max' => [self::INT32_MAX, '000000007fffffff'],
            'int32 min' => [self::INT32_MIN, 'ffffffff80000000'],
            'int64 max' => [PHP_INT_MAX, '7fffffffffffffff'],
            'int64 min' => [PHP_INT_MIN, '8000000000000000'],
        ];
    }

    /**
     * @param non-empty-string $hex
     */
    #[Test]
    #[DataProvider([self::class, 'varints'])]
    public function varintMatchesKafka(int $value, string $hex): void
    {
        $writer = new Writer();
        $writer->writeVarint($value);

        Assert::same(bin2hex((string) $writer), $hex);

        $reader = self::reader($hex);
        Assert::same($reader->readVarint(), $value);
        Assert::same($reader->remaining(), 0);
    }

    /**
     * @param non-empty-string $hex
     */
    #[Test]
    #[DataProvider([self::class, 'unsignedVarints'])]
    public function unsignedVarintMatchesKafka(int $value, string $hex): void
    {
        $writer = new Writer();
        $writer->writeUnsignedVarint($value);

        Assert::same(bin2hex((string) $writer), $hex);

        $reader = self::reader($hex);
        Assert::same($reader->readUnsignedVarint(), $value);
        Assert::same($reader->remaining(), 0);
    }

    /**
     * @param non-empty-string $hex
     */
    #[Test]
    #[DataProvider([self::class, 'int64s'])]
    public function int64MatchesKafka(int $value, string $hex): void
    {
        $writer = new Writer();
        $writer->writeInt64($value);

        Assert::same(bin2hex((string) $writer), $hex);

        $reader = self::reader($hex);
        Assert::same($reader->readInt64(), $value);
        Assert::same($reader->remaining(), 0);
    }

    #[Test]
    public function negativeUnsignedVarintTakesAll64Bits(): void
    {
        // The same bytes as writeUnsignedVarlong() of the Java client.
        $writer = new Writer();
        $writer->writeUnsignedVarint(-1);
        $writer->writeUnsignedVarint(PHP_INT_MIN);

        Assert::same(bin2hex((string) $writer), 'ffffffffffffffffff0180808080808080808001');

        Expect::exception(UnexpectedValue::class)
            ->withMessageContaining('Unsigned varint at offset 0 is larger than 9223372036854775807');

        new Reader((string) $writer)->readUnsignedVarint();
    }

    #[Test]
    public function varintThatEndsEarlyIsUnexpectedEof(): void
    {
        Expect::exception(UnexpectedEof::class)
            ->withMessageContaining('Varint at offset 1 ends after 2 byte(s)');

        $reader = self::reader('01ffff');
        Assert::same($reader->readVarint(), -1);

        $reader->readVarint();
    }

    #[Test]
    public function emptyUnsignedVarintIsUnexpectedEof(): void
    {
        Expect::exception(UnexpectedEof::class)
            ->withMessageContaining('Varint at offset 0 ends after 0 byte(s)');

        new Reader('')->readUnsignedVarint();
    }

    #[Test]
    public function varintOfElevenBytesIsUnexpectedValue(): void
    {
        Expect::exception(UnexpectedValue::class)
            ->withMessageContaining('Varint at offset 0 does not fit in 64 bits');

        self::reader(str_repeat('ff', 10) . '01')->readVarint();
    }

    #[Test]
    public function varintOverSixtyFourBitsIsUnexpectedValue(): void
    {
        Expect::exception(UnexpectedValue::class)
            ->withMessageContaining('Varint at offset 0 does not fit in 64 bits');

        self::reader(str_repeat('ff', 9) . '02')->readUnsignedVarint();
    }

    /**
     * @param non-empty-string $hex
     */
    private static function reader(string $hex): Reader
    {
        $bytes = hex2bin($hex);
        \assert($bytes !== false);

        return new Reader($bytes);
    }
}
