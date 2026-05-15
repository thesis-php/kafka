<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\ProducerRecord;

final class RandomTopicPartitionerTest
{
    #[Test]
    public function returnsZeroForSinglePartition(): void
    {
        $partitioner = new RandomPartitioner()->forTopic('x');

        Assert::int($partitioner->partition(new ProducerRecord(value: null), 1))->lessThanOrEqual(0);
    }

    #[Test]
    public function sticksToChosenPartitionUntilRotate(): void
    {
        $partitioner = new RandomPartitioner()->forTopic('x');
        $record = new ProducerRecord(value: null);
        $partition = $partitioner->partition($record, 3);

        Assert::int($partition)->between(0, 3);

        for ($i = 0; $i < 100; ++$i) {
            Assert::same($partitioner->partition($record, 3), $partition);
        }
    }

    #[Test]
    public function rotateMovesAwayFromCurrentPartition(): void
    {
        /** @var RandomTopicPartitioner $partitioner */
        $partitioner = new RandomPartitioner()->forTopic('x');
        $record = new ProducerRecord(value: null);

        $current = $partitioner->partition($record, 3);
        $partitioner->rotate($current, 3);

        $rotated = $partitioner->partition($record, 3);

        Assert::int($current)->between(0, 3);
        Assert::int($rotated)->between(0, 3);
        Assert::notSame($rotated, $current);
    }

    #[Test]
    public function rotateIgnoresForeignPartition(): void
    {
        /** @var RandomTopicPartitioner $partitioner */
        $partitioner = new RandomPartitioner()->forTopic('x');
        $record = new ProducerRecord(value: null);

        $current = $partitioner->partition($record, 3);
        $foreign = ($current + 1) % 3;

        $partitioner->rotate($foreign, 3);

        Assert::same($partitioner->partition($record, 3), $current);
    }

    #[Test]
    public function repicksIfStickyPartitionFallsOutOfRange(): void
    {
        /** @var RandomTopicPartitioner $partitioner */
        $partitioner = new RandomPartitioner()->forTopic('x');
        $record = new ProducerRecord(value: null);

        $current = $partitioner->partition($record, 3);
        $partitioner->rotate($current, 3);
        $rotated = $partitioner->partition($record, 3);

        if ($rotated === 2) {
            Assert::int($partitioner->partition($record, 2))->between(0, 2);
            Assert::notSame($partitioner->partition($record, 2), 2);

            return;
        }

        Assert::same($partitioner->partition($record, 2), $rotated);
    }
}
