<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\NullCancellation;
use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Internal\ConnectionFactory;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;
use function Amp\delay;

final readonly class PlainSaslReauthenticationTest extends TestCase
{
    #[Test]
    public function reauthenticatesBeforeOrdinaryRequestWhenSessionExpired(): void
    {
        $factory = new ConnectionFactory(new Config(
            seeds: ['kafka-plain-reauth-standalone:9092'],
            sasl: new SASLConfig([
                new PlainAuthenticator(
                    username: 'broker',
                    password: 'broker-secret',
                ),
            ]),
        ));

        $connection = $factory->open(
            Endpoint::parse('kafka-plain-reauth-standalone:9092'),
            new NullCancellation(),
        );

        $initialExpiry = $this->sessionExpiresMs($connection);

        Assert::int($initialExpiry)->greaterThan(Connection\now());

        /** @var non-negative-int $expiredAt */
        $expiredAt = Connection\now() - 1;
        $this->forceSessionExpiresMs($connection, $expiredAt);

        $response = $connection->call(
            new Connection\Call(
                request: $this->metadataRequest(),
            ),
        );

        Assert::same($response->errorCode, 0);
        Assert::count($response->brokers, 1);

        $refreshedExpiry = $this->sessionExpiresMs($connection);

        Assert::int($refreshedExpiry)->greaterThan(Connection\now());
        Assert::int($refreshedExpiry)->greaterThan($expiredAt);

        $connection->close();
    }

    #[Test]
    public function closesConnectionIfExpiredSessionBypassesRefresh(): void
    {
        $factory = new ConnectionFactory(new Config(
            seeds: ['kafka-plain-reauth-standalone:9092'],
            sasl: new SASLConfig([
                new PlainAuthenticator(
                    username: 'broker',
                    password: 'broker-secret',
                ),
            ]),
        ));

        $connection = $factory->open(
            Endpoint::parse('kafka-plain-reauth-standalone:9092'),
            new NullCancellation(),
        );

        $this->forceSessionExpiresMs($connection, 0);

        delay(2.5);

        Expect::exception(ConnectionClosed::class)
            ->fromMethod(Connection::class, 'call');

        try {
            $connection->call(
                new Connection\Call(
                    request: $this->metadataRequest(),
                ),
            );
        } finally {
            $connection->close();
        }
    }

    private function sessionExpiresMs(Connection $connection): int
    {
        $expires = $this->sessionExpiresMsProperty()->getValue($connection);
        \assert(\is_int($expires));

        return $expires;
    }

    /**
     * @param non-negative-int $expiresMs
     */
    private function forceSessionExpiresMs(Connection $connection, int $expiresMs): void
    {
        $this->sessionExpiresMsProperty()->setValue($connection, $expiresMs);
    }

    private function sessionExpiresMsProperty(): \ReflectionProperty
    {
        return new \ReflectionProperty(Connection::class, 'sessionExpiresMs');
    }
}
