<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\ProducerRecord;

final class RoundRobinTopicPartitionerTest
{
    #[Test]
    public function returnsZeroForSinglePartition(): void
    {
        $partitioner = new RoundRobinPartitioner()->forTopic('x');

        Assert::same($partitioner->partition(new ProducerRecord(), 1), 0);
    }

    #[Test]
    public function startsAtZeroAndSticksUntilRotate(): void
    {
        $partitioner = new RoundRobinPartitioner()->forTopic('x');
        $record = new ProducerRecord();

        Assert::same($partitioner->partition($record, 3), 0);

        for ($i = 0; $i < 100; ++$i) {
            Assert::same($partitioner->partition($record, 3), 0);
        }
    }

    #[Test]
    public function rotateMovesToNextPartition(): void
    {
        /** @var RoundRobinTopicPartitioner $partitioner */
        $partitioner = new RoundRobinPartitioner()->forTopic('x');
        $record = new ProducerRecord();

        Assert::same($partitioner->partition($record, 3), 0);

        $partitioner->rotate(0, 3);
        Assert::same($partitioner->partition($record, 3), 1);

        $partitioner->rotate(1, 3);
        Assert::same($partitioner->partition($record, 3), 2);

        $partitioner->rotate(2, 3);
        Assert::same($partitioner->partition($record, 3), 0);
    }

    #[Test]
    public function rotateIgnoresForeignPartition(): void
    {
        /** @var RoundRobinTopicPartitioner $partitioner */
        $partitioner = new RoundRobinPartitioner()->forTopic('x');
        $record = new ProducerRecord();

        Assert::same($partitioner->partition($record, 3), 0);

        $partitioner->rotate(1, 3);

        Assert::same($partitioner->partition($record, 3), 0);
    }

    #[Test]
    public function repicksIfStickyPartitionFallsOutOfRange(): void
    {
        /** @var RoundRobinTopicPartitioner $partitioner */
        $partitioner = new RoundRobinPartitioner()->forTopic('x');
        $record = new ProducerRecord();

        Assert::same($partitioner->partition($record, 3), 0);

        $partitioner->rotate(0, 3);
        Assert::same($partitioner->partition($record, 3), 1);

        $partitioner->rotate(1, 3);
        Assert::same($partitioner->partition($record, 3), 2);

        Assert::same($partitioner->partition($record, 2), 0);
    }
}
