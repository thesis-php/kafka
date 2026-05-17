<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\SASL\Scram\Sha256ScramAuthenticator;

#[Covers(Sha256ScramAuthenticator::class)]
final readonly class ScramSha256SaslAuthenticationTest extends TestCase
{
    #[Test]
    public function metadataWithoutSaslClosesConnection(): void
    {
        $client = $this->createScramClient();

        Expect::exception(AllBrokersFailed::class)
            ->fromMethod(Client::class, 'request')
            ->withMessageContaining('Connection closed by the broker');

        $client->request($this->metadataRequest());
    }

    #[Test]
    public function metadataWithScramSha256Succeeds(): void
    {
        $client = $this->createScramClient(
            new SASLConfig([
                new Sha256ScramAuthenticator(
                    username: 'test',
                    password: 'test-secret',
                ),
            ]),
        );

        $response = $client->request($this->metadataRequest());

        Assert::same($response->errorCode, 0);
        Assert::count($response->brokers, 1);
        \assert(\count($response->brokers) > 0);
        Assert::same($response->brokers[0]->host, 'kafka-scram-256-standalone');
        Assert::same($response->brokers[0]->port, 9092);

        $client->close();
    }

    #[Test]
    public function metadataWithWrongScramSha256PasswordFailsAuthentication(): void
    {
        $client = $this->createScramClient(
            new SASLConfig([
                new Sha256ScramAuthenticator(
                    username: 'test',
                    password: 'wrong-secret',
                ),
            ]),
        );

        Expect::exception(AllBrokersFailed::class)
            ->fromMethod(Client::class, 'request')
            ->withMessageContaining('Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256');

        $client->request($this->metadataRequest());
    }

    private function createScramClient(?SASLConfig $sasl = null): Client
    {
        return $this->createClient(
            seeds: ['kafka-scram-256-standalone:9092'],
            sasl: $sasl,
        );
    }
}
