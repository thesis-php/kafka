<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\ProducerRecord;

final class HashTopicPartitionerTest
{
    #[Test]
    public function hashesNonNullKeyLikeKafkaPartitioner(): void
    {
        $partitioner = new HashPartitioner()->forTopic('x');

        Assert::same(
            $partitioner->partition(new ProducerRecord(value: null, key: '21'), 7),
            3,
        );
    }

    #[Test]
    public function fallsBackWhenKeyIsNull(): void
    {
        $partitioner = new HashPartitioner()->forTopic('x');

        Assert::same(
            $partitioner->partition(new ProducerRecord(value: null), 3),
            0,
        );
    }

    #[Test]
    public function forwardsRotateToStickyFallbackForNullKeyFlow(): void
    {
        /** @var HashTopicPartitioner $partitioner */
        $partitioner = new HashPartitioner()->forTopic('x');

        Assert::same($partitioner->partition(new ProducerRecord(value: null), 3), 0);

        $partitioner->rotate(0, 3);

        Assert::same($partitioner->partition(new ProducerRecord(value: null), 3), 1);
    }

    #[Test]
    public function rotateDoesNotAffectPartitionChosenForKeyedRecords(): void
    {
        /** @var HashTopicPartitioner $partitioner */
        $partitioner = new HashPartitioner()->forTopic('x');

        $record = new ProducerRecord(value: null, key: '21');

        Assert::same($partitioner->partition($record, 7), 3);

        $partitioner->rotate(0, 7);

        Assert::same($partitioner->partition($record, 7), 3);
    }
}
