<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\Proxy;
use Thesis\Time\TimeSpan;
use function Amp\async;

final readonly class ProduceRetryTest
{
    /**
     * The only broker of this setup advertises 127.0.0.1:19094 on its second
     * listener. A proxy on this port puts every connection behind the proxy.
     */
    private const string PROXIED_BROKER = 'kafka-proxied-standalone:9092';
    private const int PROXIED_PORT = 19094;

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function recordIsDeliveredAfterConnectionsWereDropped(): void
    {
        $log = new TestHandler();

        Kafka::connect(self::PROXIED_BROKER)->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = self::proxiedProducer($kafka, attempts: 3);

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The produce request stays on the silent connection until the proxy drops it.
            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            self::waitForAttempts($kafka, $log, count: 2);
            $proxy->drop();

            $produced = $future->await();

            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 1]);
            Assert::same(self::attempts($log), [1, 1, 2]);

            $retries = self::records($log, 'produce failed, retrying');
            Assert::same(\count($retries), 1);
            Assert::instanceOf($retries[0]->context['exception'] ?? null, NetworkException::class);

            // The first backoff is at least 50 ms. A retry without a wait comes in less than 1 ms.
            $attempts = self::records($log, 'producing batch');
            Assert::float(self::seconds($retries[0] ?? null, $attempts[2] ?? null))->greaterThan(0.04);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceFailsAfterAllAttemptsWhenBrokerIsGone(): void
    {
        $log = new TestHandler();

        Kafka::connect(self::PROXIED_BROKER)->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = self::proxiedProducer($kafka, attempts: 3);

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->disconnect();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: 'second', topic: $topic));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, NetworkException::class);
            // The metadata refresh between two attempts fails too, but the next attempt still runs.
            Assert::same(self::attempts($log), [1, 1, 2, 3]);
            Assert::same(\count(self::records($log, 'produce failed, retrying')), 2);

            // The two backoffs are at least 50 ms and 100 ms. Attempts without a wait take a few ms.
            $attempts = self::records($log, 'producing batch');
            Assert::float(self::seconds($attempts[1] ?? null, $attempts[3] ?? null))->greaterThan(0.12);
        });
    }

    #[Test]
    public function unknownPartitionIsNotRetried(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: 'lost', topic: $topic, partition: 5));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, UnknownPartition::class);
            Assert::same([$failure->topic, $failure->partition], [$topic, 5]);
            Assert::same(\count(self::records($log, 'produce failed, retrying')), 0);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function cancelledProduceIsNotRetried(): void
    {
        $log = new TestHandler();

        Kafka::connect(self::PROXIED_BROKER)->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = self::proxiedProducer($kafka, attempts: 3);

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            self::waitForAttempts($kafka, $log, count: 2);

            // Close cancels the produce request that waits for the silent broker.
            $producer->close();

            $failure = null;

            try {
                $future->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same(self::attempts($log), [1, 1]);
            Assert::same(\count(self::records($log, 'produce failed, retrying')), 0);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceCancelledDuringBackoffIsNotRetried(): void
    {
        $log = new TestHandler();

        /** @var ?Producer $producer the test sets it, the logger reads it */
        $producer = null;

        // Close the producer while it waits before the next attempt.
        $logger = new Logger('test', [$log], [static function (LogRecord $record) use (&$producer): LogRecord {
            if ($record->message === 'produce failed, retrying' && $producer instanceof Producer) {
                async($producer->close(...))->ignore();
            }

            return $record;
        }]);

        Kafka::connect(self::PROXIED_BROKER)->logger($logger)->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log, &$producer): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = self::proxiedProducer($kafka, attempts: 3);

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->disconnect();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: 'second', topic: $topic));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same(self::attempts($log), [1, 1]);
            Assert::same(\count(self::records($log, 'produce failed, retrying')), 1);
        });
    }

    #[Test]
    public function errorThatCannotBeRetriedFailsOnFirstAttempt(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic(configs: ['max.message.bytes' => '1024']);
            $producer = $kafka->producer();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: str_repeat('x', 4_096), topic: $topic));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, ProtocolError::class);
            Assert::same(
                array_map(static fn(Error $error) => $error->code(), $failure->errors->errors),
                [ErrorCode::MessageTooLarge],
            );

            // The partition sends one batch at a time. So the next record waits
            // until the producer is done with the first one.
            Assert::same($producer->produceSync(new Record(value: 'small', topic: $topic))->offset, 0);
            Assert::same(self::attempts($log), [1, 1]);
        });
    }

    /**
     * @param positive-int $attempts
     * @return array{Proxy, Producer}
     */
    private static function proxiedProducer(Kafka $kafka, int $attempts): array
    {
        $proxy = $kafka->proxy(new Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
        $client = $kafka->createClient(seeds: [$proxy->endpoint]);
        $producer = $kafka->producer(new Producer\Config(produceAttempts: $attempts), client: $client);

        return [$proxy, $producer];
    }

    /**
     * @param positive-int $count
     */
    private static function waitForAttempts(Kafka $kafka, TestHandler $log, int $count): void
    {
        $kafka
            ->poll(static fn(Cancellation $_) => self::attempts($log))
            ->expecting("{$count} produce attempts in the log")
            ->describedBy(static fn(array $attempts) => json_encode($attempts, JSON_THROW_ON_ERROR))
            ->until(static fn(array $attempts) => \count($attempts) >= $count);
    }

    /**
     * @return list<mixed> the attempt of each produce request in the log
     */
    private static function attempts(TestHandler $log): array
    {
        return array_map(
            static fn(LogRecord $record) => $record->context['attempt'] ?? null,
            self::records($log, 'producing batch'),
        );
    }

    /**
     * @return list<LogRecord>
     */
    private static function records(TestHandler $log, string $message): array
    {
        return array_values(array_filter(
            $log->getRecords(),
            static fn(LogRecord $record) => $record->message === $message,
        ));
    }

    private static function seconds(?LogRecord $from, ?LogRecord $to): float
    {
        Assert::instanceOf($from, LogRecord::class);
        Assert::instanceOf($to, LogRecord::class);

        return (float) $to->datetime->format('U.u') - (float) $from->datetime->format('U.u');
    }
}
