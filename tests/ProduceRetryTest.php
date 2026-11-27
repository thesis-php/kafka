<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Time\TimeSpan;
use function Amp\async;

final readonly class ProduceRetryTest
{
    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function recordIsDeliveredAfterConnectionsWereDropped(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The produce request stays on the silent connection until the proxy drops it.
            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            $log->waitForAttempts($kafka, count: 2);
            $proxy->drop();

            $produced = $future->await();

            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 1]);
            Assert::same($log->attempts(), [1, 1, 2]);

            $retries = $log->records('produce failed, retrying');
            Assert::same(\count($retries), 1);
            Assert::instanceOf($retries[0]->context['exception'] ?? null, NetworkException::class);

            // The first backoff is at least 50 ms. A retry without a wait comes in less than 1 ms.
            $attempts = $log->records('producing batch');
            Assert::float(self::seconds($retries[0] ?? null, $attempts[2] ?? null))->greaterThan(0.04);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceFailsAfterAllAttemptsWhenBrokerIsGone(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

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
            Assert::same($log->attempts(), [1, 1, 2, 3]);
            Assert::same(\count($log->records('produce failed, retrying')), 2);

            // The two backoffs are at least 50 ms and 100 ms. Attempts without a wait take a few ms.
            $attempts = $log->records('producing batch');
            Assert::float(self::seconds($attempts[1] ?? null, $attempts[3] ?? null))->greaterThan(0.12);
        });
    }

    #[Test]
    public function unknownPartitionIsNotRetried(): void
    {
        $log = new ProduceLog();

        Kafka::connect()->logger($log->logger())->test(static function (Kafka $kafka) use ($log): void {
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
            Assert::same(\count($log->records('produce failed, retrying')), 0);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function cancelledProduceIsNotRetried(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            $log->waitForAttempts($kafka, count: 2);

            // The time limit of close() cancels the produce request that waits for the silent broker.
            $producer->close(new TimeoutCancellation(0.2));

            $failure = null;

            try {
                $future->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same($log->attempts(), [1, 1]);
            Assert::same(\count($log->records('produce failed, retrying')), 0);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceCancelledDuringBackoffIsNotRetried(): void
    {
        /** @var ?Producer $producer the test sets it, the logger reads it */
        $producer = null;

        // Close the producer at once while it waits before the next attempt.
        $log = new ProduceLog();
        $logger = $log->logger(static function (LogRecord $record) use (&$producer): LogRecord {
            if ($record->message === 'produce failed, retrying' && $producer instanceof Producer) {
                $now = new DeferredCancellation();
                $now->cancel();
                async($producer->close(...), $now->getCancellation())->ignore();
            }

            return $record;
        });

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($logger)->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka) use ($log, &$producer): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->disconnect();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: 'second', topic: $topic));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same($log->attempts(), [1, 1]);
            Assert::same(\count($log->records('produce failed, retrying')), 1);
        });
    }

    #[Test]
    public function errorThatCannotBeRetriedFailsOnFirstAttempt(): void
    {
        $log = new ProduceLog();

        Kafka::connect()->logger($log->logger())->test(static function (Kafka $kafka) use ($log): void {
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
            Assert::same($log->attempts(), [1, 1]);
        });
    }

    private static function seconds(?LogRecord $from, ?LogRecord $to): float
    {
        Assert::instanceOf($from, LogRecord::class);
        Assert::instanceOf($to, LogRecord::class);

        return (float) $to->datetime->format('U.u') - (float) $from->datetime->format('U.u');
    }
}
