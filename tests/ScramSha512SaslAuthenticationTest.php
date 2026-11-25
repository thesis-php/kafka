<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\Exception\SaslAuthenticationFailed;
use Thesis\Kafka\SASL\Scram\Sha512ScramAuthenticator;
use Thesis\Kafka\Testing\Kafka;

final readonly class ScramSha512SaslAuthenticationTest
{
    private const string BROKER = 'kafka-scram-512-standalone:9092';

    #[Test]
    public function metadataWithoutSaslClosesConnection(): void
    {
        Kafka::connect(self::BROKER)->test(static function (Kafka $kafka): void {
            Expect::exception(AllBrokersFailed::class)
                ->fromMethod(Client::class, 'request')
                ->withMessageContaining('Connection closed by the broker');

            $kafka->client->request(Kafka::metadataRequest());
        });
    }

    #[Test]
    public function metadataWithScramSha512Succeeds(): void
    {
        Kafka::connect(self::BROKER)
            ->sasl(new SASLConfig([
                new Sha512ScramAuthenticator(
                    username: 'test',
                    password: 'test-secret',
                ),
            ]))
            ->test(static function (Kafka $kafka): void {
                $response = $kafka->client->request(Kafka::metadataRequest());

                Assert::same($response->errorCode, 0);
                Assert::count($response->brokers, 1);
                \assert(\count($response->brokers) > 0);
                Assert::same($response->brokers[0]->host, 'kafka-scram-512-standalone');
                Assert::same($response->brokers[0]->port, 9092);
            });
    }

    #[Test]
    public function metadataWithWrongScramSha512PasswordFailsAuthentication(): void
    {
        Kafka::connect(self::BROKER)
            ->sasl(new SASLConfig([
                new Sha512ScramAuthenticator(
                    username: 'test',
                    password: 'wrong-secret',
                ),
            ]))
            ->test(static function (Kafka $kafka): void {
                // A wrong password is not a network error: the client does not try other brokers.
                Expect::exception(SaslAuthenticationFailed::class)
                    ->withMessageContaining('Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512');

                $kafka->client->request(Kafka::metadataRequest());
            });
    }
}
