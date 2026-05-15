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
use Thesis\Kafka\Internal\Time;
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

        $initialExpiry = $this->sessionExpires($connection);

        Assert::int($initialExpiry)->greaterThan(Time\now()->milliseconds);

        /** @var non-negative-int $expiredAt */
        $expiredAt = Time\now()->milliseconds - 1;
        $this->forceSessionExpires($connection, $expiredAt);

        $response = $connection->call(
            new Connection\Call(
                request: $this->metadataRequest(),
            ),
        );

        Assert::same($response->errorCode, 0);
        Assert::count($response->brokers, 1);

        $refreshedExpiry = $this->sessionExpires($connection);

        Assert::int($refreshedExpiry)->greaterThan(Time\now()->milliseconds);
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

        $this->forceSessionExpires($connection, null);

        delay(2.5);

        Expect::exception(ConnectionClosed::class)
            ->fromMethod(Connection::class, 'call');

        $connection->call(
            new Connection\Call(
                request: $this->metadataRequest(),
            ),
        );
    }

    private function sessionExpires(Connection $connection): int
    {
        /** @var ?Time\Duration $expires */
        $expires = $this->sessionExpiresProperty()->getValue($connection);
        \assert($expires !== null);

        return $expires->milliseconds;
    }

    /**
     * @param ?non-negative-int $expiresMs
     */
    private function forceSessionExpires(Connection $connection, ?int $expiresMs): void
    {
        $this->sessionExpiresProperty()->setValue(
            $connection,
            $expiresMs === null ? null : new Time\Duration($expiresMs),
        );
    }

    private function sessionExpiresProperty(): \ReflectionProperty
    {
        return new \ReflectionProperty(Connection::class, 'sessionExpires');
    }
}
