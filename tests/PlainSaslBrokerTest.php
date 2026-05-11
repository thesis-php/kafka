<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;

final readonly class PlainSaslBrokerTest extends TestCase
{
    #[Test]
    public function metadataWithoutSaslClosesConnection(): void
    {
        $client = $this->createPlainClient();

        Expect::exception(ConnectionClosed::class)
            ->fromMethod(Client::class, 'request');

        $client->request($this->metadataRequest());
    }

    #[Test]
    public function metadataWithPlainSaslSucceeds(): void
    {
        $client = $this->createPlainClient(
            new SASLConfig([
                new PlainAuthenticator(
                    username: 'test',
                    password: 'test-secret',
                ),
            ]),
        );

        $response = $client->request($this->metadataRequest());

        Assert::same($response->errorCode, 0);
        Assert::count($response->brokers, 1);
        \assert(\count($response->brokers) > 0);
        Assert::same($response->brokers[0]->host, 'kafka-plain-standalone');
        Assert::same($response->brokers[0]->port, 9092);

        $client->close();
    }

    private function createPlainClient(?SASLConfig $sasl = null): Client
    {
        return new Client(new Config(
            seeds: ['kafka-plain-standalone:9092'],
            sasl: $sasl,
        ));
    }

    private function metadataRequest(): Generated\MetadataRequest
    {
        return new Generated\MetadataRequest(
            topics: null,
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        );
    }
}
