<?php

declare(strict_types=1);

namespace Thesis\Kafka\Hasher;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Data\DataSet;
use Testo\Test;
use Thesis\Kafka\Producer\Hasher\Murmur2Hasher;

/**
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java#L106
 */
#[Covers(Murmur2Hasher::class)]
final class Murmur2HasherTest
{
    /**
     * @param non-empty-string $key
     */
    #[Test]
    #[DataSet(['21', -973932308])]
    #[DataSet(['foobar', -790332482])]
    #[DataSet(['a-little-bit-long-string', -985981536])]
    #[DataSet(['a-little-bit-longer-string', -1486304829])]
    #[DataSet(['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8', -58897971])]
    #[DataSet(['abc', 479470107])]
    public function hashesLikeKafkaJavaClient(string $key, int $hash): void
    {
        $hasher = new Murmur2Hasher();
        Assert::same($hasher->hash($key), $hash);
    }
}
