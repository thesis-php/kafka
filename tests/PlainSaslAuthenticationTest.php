<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;
use Thesis\Kafka\Testing\Kafka;

final readonly class PlainSaslAuthenticationTest
{
    private const string BROKER = 'kafka-plain-standalone:9092';

    #[Test]
    public function metadataWithoutSaslClosesConnection(): void
    {
        Kafka::connect(self::BROKER)->test(static function (Kafka $kafka): void {
            Expect::exception(AllBrokersFailed::class)
                ->withMessageContaining('Connection closed by the broker')
                ->fromMethod(Client::class, 'request');

            $kafka->client->request(Kafka::metadataRequest());
        });
    }

    #[Test]
    public function metadataWithPlainSaslSucceeds(): void
    {
        Kafka::connect(self::BROKER)
            ->sasl(new SASLConfig([
                new PlainAuthenticator(
                    username: 'test',
                    password: 'test-secret',
                ),
            ]))
            ->test(static function (Kafka $kafka): void {
                $response = $kafka->client->request(Kafka::metadataRequest());

                Assert::same($response->errorCode, 0);
                Assert::count($response->brokers, 1);
                \assert(\count($response->brokers) > 0);
                Assert::same($response->brokers[0]->host, 'kafka-plain-standalone');
                Assert::same($response->brokers[0]->port, 9092);
            });
    }

    #[Test]
    public function metadataWithWrongPlainPasswordFailsAuthentication(): void
    {
        Kafka::connect(self::BROKER)
            ->sasl(new SASLConfig([
                new PlainAuthenticator(
                    username: 'test',
                    password: 'wrong-secret',
                ),
            ]))
            ->test(static function (Kafka $kafka): void {
                Expect::exception(AllBrokersFailed::class)
                    ->fromMethod(Client::class, 'request')
                    ->withMessageContaining('Invalid username or password');

                $kafka->client->request(Kafka::metadataRequest());
            });
    }
}
