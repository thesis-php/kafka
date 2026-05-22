<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Time\TimeSpan;

#[Covers(ProducerConfig::class)]
final readonly class ProducerConfigTest
{
    #[Test]
    public function defaults(): void
    {
        $config = new ProducerConfig();

        Assert::same($config->batchSize, ProducerConfig::DEFAULT_BATCH_SIZE);
        Assert::true($config->linger->isEqualTo(TimeSpan::fromMilliseconds(ProducerConfig::DEFAULT_LINGER_MS)));
        Assert::true($config->produceTimeout->isEqualTo(TimeSpan::fromSeconds(ProducerConfig::DEFAULT_PRODUCE_TIMEOUT_SECS)));
    }

    #[Test]
    public function manualFlushDisablesLingerAndBatchSize(): void
    {
        $config = new ProducerConfig(manualFlush: true);

        Assert::same($config->batchSize, -1);
        Assert::true($config->linger->isZero());
    }
}
