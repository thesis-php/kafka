<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Hasher;

use Thesis\Kafka\Producer\Hasher;

/**
 * MurmurHash2, the exact variant used by the Apache Kafka Java client.
 * Compatibility with Java / librdkafka / franz-go matters: records with the same key must end up on the same partition
 * regardless of which client wrote them, otherwise per-key ordering guarantees break across the cluster.
 *
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L503
 * @api
 */
final class Murmur2Hasher implements Hasher
{
    private const int SEED = 0x9747B28C;
    private const int M    = 0x5BD1E995;
    private const int R    = 24;
    private const int MASK = 0xFFFFFFFF;

    #[\Override]
    public function hash(string $key): int
    {
        $length = \strlen($key);

        /** @var list<int> $bytes */
        $bytes = unpack('C*', $key);

        $h = (self::SEED ^ $length) & self::MASK;

        $blocks = $length >> 2;
        for ($i = 0; $i < $blocks; ++$i) {
            $i4 = $i * 4 + 1;
            $k = $bytes[$i4] // @phpstan-ignore offsetAccess.notFound
                | ($bytes[$i4 + 1] << 8) // @phpstan-ignore offsetAccess.notFound
                | ($bytes[$i4 + 2] << 16) // @phpstan-ignore offsetAccess.notFound
                | ($bytes[$i4 + 3] << 24); // @phpstan-ignore offsetAccess.notFound

            $k = ($k * self::M) & self::MASK;
            // Java's "k ^= k >>> r" — since $k is already masked to 32 bits,
            // PHP's >> behaves as unsigned shift on it.
            $k = ($k ^ ($k >> self::R)) & self::MASK;
            $k = ($k * self::M) & self::MASK;
            $h = ($h * self::M) & self::MASK;
            $h ^= $k;
        }

        $tail = $length & 3;
        $tailOffset = ($length & ~3) + 1;

        if ($tail >= 3) {
            $h ^= $bytes[$tailOffset + 2] << 16; // @phpstan-ignore offsetAccess.notFound
        }

        if ($tail >= 2) {
            $h ^= $bytes[$tailOffset + 1] << 8; // @phpstan-ignore offsetAccess.notFound
        }

        if ($tail >= 1) {
            $h ^= $bytes[$tailOffset]; // @phpstan-ignore offsetAccess.notFound
            $h = ($h * self::M) & self::MASK;
        }

        $h ^= ($h & self::MASK) >> 13;
        $h = ($h * self::M) & self::MASK;
        $h ^= ($h & self::MASK) >> 15;

        return $h > 0x7FFFFFFF ? $h - 0x100000000 : $h;
    }
}
