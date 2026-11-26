<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\Proxy;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

final readonly class ConsumerCancellationTest
{
    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function cancelledCoordinatorRequestKeepsCoordinator(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            $groupId = $kafka->groupId();
            // The coordinator holds a JoinGroup at most this long.
            $rebalanceTimeout = TimeSpan::fromSeconds(1.5);

            // The first member hears of a rebalance soon.
            $first = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: TimeSpan::fromSeconds(0.3));
            $first->consume();
            self::waitFor($kafka, $log, 'joined group', 1);

            // The second member does not heartbeat for 5 s. So it does not rejoin
            // at the next rebalance, and the coordinator holds the JoinGroup of the
            // first member until the rebalance timeout.
            $second = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: TimeSpan::fromSeconds(5));
            $second->consume();
            self::waitFor($kafka, $log, 'joined group', 3);

            $signals = self::count($log, 'rebalance signalled by heartbeat');

            $third = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: TimeSpan::fromSeconds(5));
            $third->consume();
            self::waitFor($kafka, $log, 'rebalance signalled by heartbeat', $signals + 1);

            // Gives the first member time to send its JoinGroup.
            delay(0.2);

            $discovered = self::count($log, 'group coordinator discovered');
            Assert::same($discovered, 3);

            // close() cancels the held JoinGroup. Then it sends LeaveGroup to the same coordinator.
            $first->close();

            Assert::same(self::count($log, 'group coordinator invalidated'), 0);
            Assert::same(self::count($log, 'group coordinator discovered'), $discovered);
            Assert::same(self::count($log, 'leave group failed'), 0);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function cancelledCallerDoesNotFailSetupOfOtherCaller(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $kafka->producer()->produceSync([
                new Producer\Record(value: 'v-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'v-1', topic: $topic, partition: 0),
            ]);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            $cancellation = new DeferredCancellation();
            $limit = new TimeoutCancellation(10);

            // Both calls wait for the same setup of the consumer.
            $cancelled = async(static fn() => $consumer->consume(ConsumeMode::Poll, $cancellation->getCancellation()));
            $other = async(static function () use ($consumer, $limit): array {
                $values = [];

                foreach ($consumer->consume(ConsumeMode::Poll, $limit) as $batch) {
                    foreach ($batch as $record) {
                        $values[] = $record->value;
                    }
                }

                return $values;
            });

            delay(0);
            $cancellation->cancel();

            [$errors, $results] = Future\awaitAll([$cancelled, $other]);

            Assert::same(array_map(get_debug_type(...), $errors), [0 => CancelledException::class]);
            Assert::same($results, [1 => ['v-0', 'v-1']]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function setupEndsWhenBrokerDoesNotAnswer(): void
    {
        $log = new TestHandler();
        /** @var ?Proxy $stalling */
        $stalling = null;

        // Each retry of the metadata request makes the open connections silent.
        // So the next attempt of the setup waits for an answer that never comes.
        $logger = new Logger('test', [$log], [
            static function (LogRecord $record) use (&$stalling): LogRecord {
                if ($record->message === 'operation failed, retrying' && ($record->context['operation'] ?? null) === 'metadata') {
                    $stalling?->stall();
                }

                return $record;
            },
        ]);

        Kafka::connect('kafka-proxied-standalone:9092')->logger($logger)->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka) use ($log, &$stalling): void {
            $proxy = $kafka->proxy(new Endpoint('kafka-proxied-standalone', 9094), 19094);
            $client = $kafka->createClient(seeds: [$proxy->endpoint], requestTimeout: TimeSpan::fromSeconds(1));

            // The topic does not exist yet, so the setup retries the metadata request.
            $topic = $kafka->topicName();
            $consumer = $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest), $client);

            $stalling = $proxy;
            $cancellation = new DeferredCancellation();
            $cancelled = async(static fn() => $consumer->consume(ConsumeMode::Poll, $cancellation->getCancellation()));

            delay(0);
            $cancellation->cancel();

            [$errors] = Future\awaitAll([$cancelled]);
            Assert::same(array_map(get_debug_type(...), $errors), [CancelledException::class]);

            // Nobody waits for the setup now. It must end by itself.
            self::waitFor($kafka, $log, 'metadata refresh failed', 1);
            $stalling = null;

            $kafka->topic(name: $topic);
            $kafka->producer()->produceSync([
                new Producer\Record(value: 'v-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'v-1', topic: $topic, partition: 0),
            ]);

            $values = $kafka->poll(static function (Cancellation $cancellation) use ($consumer): array {
                $values = [];

                try {
                    foreach ($consumer->consume(ConsumeMode::Poll, $cancellation) as $batch) {
                        foreach ($batch as $record) {
                            $values[] = $record->value;
                        }
                    }
                } catch (CancelledException $e) {
                    // The first setup leaves a silent connection open. The next setup
                    // waits for its request timeout, closes the connection and fails.
                    if ($cancellation->isRequested()) {
                        throw $e;
                    }
                }

                return $values;
            })
                ->expecting('records of the topic')
                ->until(static fn(array $values) => $values !== []);

            Assert::same($values, ['v-0', 'v-1']);
        });
    }

    /**
     * @param non-empty-string $message
     */
    private static function waitFor(Kafka $kafka, TestHandler $log, string $message, int $count): void
    {
        $kafka->poll(static fn(Cancellation $_) => self::count($log, $message))
            ->expecting(\sprintf('%d log records "%s"', $count, $message))
            ->until(static fn(int $seen) => $seen >= $count);
    }

    private static function count(TestHandler $log, string $message): int
    {
        return \count(array_filter(
            $log->getRecords(),
            static fn($record) => $record->message === $message,
        ));
    }
}
