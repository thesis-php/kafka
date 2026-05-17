<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;

#[Covers(PlainAuthenticator::class)]
final readonly class PlainSaslAuthenticationTest extends TestCase
{
    #[Test]
    public function metadataWithoutSaslClosesConnection(): void
    {
        $client = $this->createPlainClient();

        Expect::exception(AllBrokersFailed::class)
            ->withMessageContaining('Connection closed by the broker')
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

    #[Test]
    public function metadataWithWrongPlainPasswordFailsAuthentication(): void
    {
        $client = $this->createPlainClient(
            new SASLConfig([
                new PlainAuthenticator(
                    username: 'test',
                    password: 'wrong-secret',
                ),
            ]),
        );

        Expect::exception(AllBrokersFailed::class)
            ->fromMethod(Client::class, 'request')
            ->withMessageContaining('Invalid username or password');

        $client->request($this->metadataRequest());
    }

    private function createPlainClient(?SASLConfig $sasl = null): Client
    {
        return $this->createClient(
            seeds: ['kafka-plain-standalone:9092'],
            sasl: $sasl,
        );
    }
}
