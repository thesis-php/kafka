<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;

final readonly class LegacyApiVersionsBrokerTest
{
    #[Test]
    public function connectsToLegacyBrokerUsingApiVersionsFallback(): void
    {
        Kafka::connect('kafka-apiversions-legacy:9092')->test(static function (Kafka $kafka): void {
            $response = $kafka->client->request(new Generated\ApiVersionsRequest(
                clientSoftwareName: 'thesis-kafka-tests',
                clientSoftwareVersion: 'dev',
            ));

            Assert::same($response->errorCode, 0);
            Assert::true(\count($response->apiKeys) > 0);
        });
    }
}
