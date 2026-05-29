<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Time\TimeSpan;

#[Covers(Config::class)]
final readonly class ConfigTest
{
    #[Test]
    public function defaults(): void
    {
        $config = new Config();

        Assert::same($config->batchSize, Config::DEFAULT_BATCH_SIZE);
        Assert::true($config->linger->isEqualTo(TimeSpan::fromMilliseconds(Config::DEFAULT_LINGER_MS)));
        Assert::true($config->produceTimeout->isEqualTo(TimeSpan::fromSeconds(Config::DEFAULT_PRODUCE_TIMEOUT_SECS)));
    }
}
