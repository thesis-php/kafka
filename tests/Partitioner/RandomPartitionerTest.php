<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\ProducerRecord;

final class RandomPartitionerTest
{
    #[Test]
    public function returnsZeroForSinglePartition(): void
    {
        $partitioner = new RandomPartitioner();

        Assert::int($partitioner->partition(new ProducerRecord(), 1))->lessThanOrEqual(0);
    }

    #[Test]
    public function returnsPartitionWithinRange(): void
    {
        $partitioner = new RandomPartitioner();
        $record = new ProducerRecord();

        for ($i = 0; $i < 100; ++$i) {
            $partition = $partitioner->partition($record, 3);

            Assert::int($partition)->between(0, 3);
        }
    }
}
