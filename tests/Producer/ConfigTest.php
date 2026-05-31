<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Exception\InvalidConfiguration;
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

    #[Test]
    public function idempotentWithInvalidAcks(): void
    {
        Expect::exception(new InvalidConfiguration('Idempotent producer requires Acks::All'));

        new Config(
            acks: Acks::Leader,
            idempotent: true,
        );
    }

    #[Test]
    public function idempotentWithInvalidMaxInflightProduceRequests(): void
    {
        Expect::exception(new InvalidConfiguration('Idempotent producer requires $maxInflightProduceRequest=1'));

        new Config(
            maxInflightProduceRequest: 5,
            idempotent: true,
        );
    }
}
