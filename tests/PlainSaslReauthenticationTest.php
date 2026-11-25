<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Internal\Time;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\Setup;
use function Amp\delay;

final readonly class PlainSaslReauthenticationTest
{
    #[Test]
    public function reauthenticatesBeforeOrdinaryRequestWhenSessionExpired(): void
    {
        self::broker()->test(static function (Kafka $kafka): void {
            $connection = $kafka->connection();

            $initialExpiry = self::sessionExpires($connection);

            Assert::int($initialExpiry)->greaterThan(Time\now()->milliseconds);

            /** @var non-negative-int $expiredAt */
            $expiredAt = Time\now()->milliseconds - 1;
            self::forceSessionExpires($connection, $expiredAt);

            $response = $connection->call(
                new Connection\Call(
                    request: Kafka::metadataRequest(),
                ),
            );

            Assert::same($response->errorCode, 0);
            Assert::count($response->brokers, 1);

            $refreshedExpiry = self::sessionExpires($connection);

            Assert::int($refreshedExpiry)->greaterThan(Time\now()->milliseconds);
            Assert::int($refreshedExpiry)->greaterThan($expiredAt);
        });
    }

    #[Test]
    public function closesConnectionIfExpiredSessionBypassesRefresh(): void
    {
        self::broker()->test(static function (Kafka $kafka): void {
            $connection = $kafka->connection();

            self::forceSessionExpires($connection, null);

            delay(2.5);

            Expect::exception(ConnectionClosed::class)
                ->withMessageContaining('Connection closed by the broker');

            $connection->call(
                new Connection\Call(
                    request: Kafka::metadataRequest(),
                ),
            );
        });
    }

    private static function broker(): Setup
    {
        return Kafka::connect('kafka-plain-reauth-standalone:9092')
            ->sasl(new SASLConfig([
                new PlainAuthenticator(
                    username: 'broker',
                    password: 'broker-secret',
                ),
            ]));
    }

    private static function sessionExpires(Connection $connection): int
    {
        /** @var ?Time\Duration $expires */
        $expires = self::sessionExpiresProperty()->getValue($connection);
        \assert($expires !== null);

        return $expires->milliseconds;
    }

    /**
     * @param ?non-negative-int $expiresMs
     */
    private static function forceSessionExpires(Connection $connection, ?int $expiresMs): void
    {
        self::sessionExpiresProperty()->setValue(
            $connection,
            $expiresMs === null ? null : new Time\Duration($expiresMs),
        );
    }

    private static function sessionExpiresProperty(): \ReflectionProperty
    {
        return new \ReflectionProperty(Connection::class, 'sessionExpires');
    }
}
