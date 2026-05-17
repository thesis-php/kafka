<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated;

#[Covers(Client::class)]
final readonly class LegacyApiVersionsBrokerTest extends TestCase
{
    #[Test]
    public function connectsToLegacyBrokerUsingApiVersionsFallback(): void
    {
        $client = new Client(new Config(
            seeds: ['kafka-apiversions-legacy:9092'],
        ));

        $response = $client->request(new Generated\ApiVersionsRequest(
            clientSoftwareName: 'thesis-kafka-tests',
            clientSoftwareVersion: 'dev',
        ));

        Assert::same($response->errorCode, 0);
        Assert::true(\count($response->apiKeys) > 0);

        $client->close();
    }
}
