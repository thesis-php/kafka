<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\BatchFull;
use Thesis\Kafka\Exception\ProducerClosed;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

final readonly class ProducerCloseTest
{
    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function closeWaitsForBatchInFlight(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            $log->waitForAttempts($kafka, count: 2);

            $close = async($producer->close(...));
            delay(0.3);

            Assert::false($close->isComplete());
            Assert::false($future->isComplete());

            // The retry delivers the record on a new connection.
            $proxy->drop();
            $close->await();

            Assert::true($future->isComplete());
            $produced = $future->await();
            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 1]);
            Assert::same($log->attempts(), [1, 1, 2]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function closeWithCancellationFailsUndeliveredRecords(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $inFlight = $producer->produce(new Record(value: 'second', topic: $topic));
            $log->waitForAttempts($kafka, count: 2);

            // The partition sends one batch at a time, so this batch waits in the queue.
            $queued = $producer->produce(new Record(value: 'third', topic: $topic));
            delay(0.05);

            $producer->close(new TimeoutCancellation(0.3));

            Assert::same(self::failures([$inFlight, $queued]), [CancelledException::class, CancelledException::class]);
            // The queued batch is not sent after the cancellation.
            Assert::same($log->attempts(), [1, 1]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function secondCloseWaitsForFirst(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            $log->waitForAttempts($kafka, count: 2);

            $first = async($producer->close(...));
            $second = async($producer->close(...));
            delay(0.3);

            Assert::false($first->isComplete());
            Assert::false($second->isComplete());

            // A closed producer has nothing to flush, so this does not wait for the broker.
            $producer->flush();

            $proxy->drop();
            $second->await();

            Assert::true($first->isComplete());
            Assert::same($future->await()->offset, 1);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function flushWaitsForReleasedBatch(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(5),
                produceAttempts: 3,
            ));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();
            $future = $producer->produce(new Record(value: 'second', topic: $topic));
            // The linger has released the batch, and its request waits for the silent broker.
            $log->waitForAttempts($kafka, count: 2);

            $flush = async($producer->flush(...));
            delay(0.3);

            Assert::false($flush->isComplete());

            $proxy->drop();
            $flush->await();

            Assert::true($future->isComplete());
            Assert::same($future->await()->offset, 1);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceThatWaitsForMetadataFailsAtClose(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $first = $kafka->topic();
            $second = $kafka->topic();
            $other = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(50),
            ));

            // The first lookup goes to the seed. The second one opens the connection
            // for metadata to the broker itself, and the stall makes it silent.
            Assert::same($producer->produceSync(new Record(value: 'first', topic: $first))->offset, 0);
            Assert::same($producer->produceSync(new Record(value: 'second', topic: $second))->offset, 0);

            $proxy->stall();

            $errors = $kafka->loopErrorsDuring(static function () use ($producer, $other): void {
                // The producer asks the silent broker for the metadata of the other topic.
                $pending = async(static function () use ($producer, $other): void {
                    $producer->produce(new Record(value: 'lost', topic: $other));
                });
                delay(0.2);

                Assert::false($pending->isComplete());

                $producer->close(new TimeoutCancellation(5));

                $failure = null;

                try {
                    $pending->await();
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                Assert::instanceOf($failure, ProducerClosed::class);

                // A batch or a linger timer of the failed produce would act now.
                delay(0.2);
            });

            Assert::same(self::describe($errors), []);
        });
    }

    #[Test]
    public function closeReleasesEverything(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $produced = null;

            $left = $kafka->loopWorkAfter(static function () use ($kafka, $topic, &$produced): void {
                $producer = $kafka->producer();
                $produced = $producer->produceSync(new Record(value: 'r0', topic: $topic));
                $producer->close();
            });

            Assert::same($left, []);
            Assert::instanceOf($produced, Produced::class);
            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 0]);
        });
    }

    #[Test]
    public function closeDeliversEveryBatch(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);
            $producer = $kafka->producer(new Producer\Config(
                linger: TimeSpan::fromSeconds(3),
                batchSize: 256,
            ));

            /** @var list<Future<Produced>> $futures */
            $futures = [];

            for ($i = 0; $i < 12; ++$i) {
                // Two records do not fit into one batch. So each record releases the batch before it.
                $futures[] = $producer->produce(new Record(value: str_repeat((string) $i, 100), topic: $topic, partition: $i % 3));
            }

            $producer->close();

            $offsets = [];

            foreach ($futures as $future) {
                Assert::true($future->isComplete());
                $produced = $future->await();
                $offsets[] = [$produced->partition, $produced->offset];
            }

            Assert::same($offsets, [
                [0, 0], [1, 0], [2, 0],
                [0, 1], [1, 1], [2, 1],
                [0, 2], [1, 2], [2, 2],
                [0, 3], [1, 3], [2, 3],
            ]);

            $consumer = $kafka->groupConsumer($topic);
            $records = $kafka->consume($consumer, count: 12);
            $values = array_map(static fn(Consumer\Record $record) => $record->value, $records);
            sort($values);

            $expected = array_map(static fn(int $i) => str_repeat((string) $i, 100), range(0, 11));
            sort($expected);

            Assert::same($values, $expected);
        });
    }

    #[Test]
    public function closedProducerRejectsRecords(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $used = $kafka->topic();
            $new = $kafka->topic();
            $producer = $kafka->producer();

            Assert::same($producer->produceSync(new Record(value: 'r0', topic: $used))->offset, 0);
            $producer->close();

            $failures = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($producer, $used, $new, &$failures): void {
                $calls = [
                    static fn() => $producer->produce(new Record(value: 'r1', topic: $used)),
                    static fn() => $producer->produceSync(new Record(value: 'r1', topic: $used)),
                    static fn() => $producer->produce(new Record(value: 'r1', topic: $new)),
                    static fn() => $producer->produceSync(new Record(value: 'r1', topic: $new)),
                    static fn() => $producer->produce([new Record(value: 'r1', topic: $new)]),
                ];

                foreach ($calls as $call) {
                    try {
                        $call();
                        $failures[] = null;
                    } catch (\Throwable $e) {
                        $failures[] = $e::class;
                    }
                }

                $producer->flush();
                $producer->close();

                // The linger timer of a batch would fire now.
                delay(0.1);
            });

            Assert::same(self::describe($errors), []);
            Assert::same($failures, array_fill(0, 5, ProducerClosed::class));
        });
    }

    #[Test]
    public function bulkProduceKeepsFutureOfFullBatch(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer(new Producer\Config(
                batchSize: 400,
                manualFlush: true,
            ));

            $futures = $producer->produce([
                new Record(value: str_repeat('a', 180), topic: $topic),
                new Record(value: str_repeat('b', 180), topic: $topic),
                new Record(value: 'c', topic: $topic),
            ]);

            Assert::count($futures, 3);
            \assert(\count($futures) === 3);

            $producer->flush();

            Assert::same(self::failures($futures), [null, BatchFull::class, null]);
            Assert::same($futures[0]->await()->offset, 0);
            Assert::same($futures[2]->await()->offset, 1);
        });
    }

    #[Test]
    public function bulkProduceKeepsFutureOfUnknownPartition(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $futures = $producer->produce([
                new Record(value: 'a', topic: $topic, partition: 0),
                new Record(value: 'b', topic: $topic, partition: 5),
                new Record(value: 'c', topic: $topic, partition: 0),
            ]);

            Assert::count($futures, 3);
            \assert(\count($futures) === 3);
            Assert::same(self::failures($futures), [null, UnknownPartition::class, null]);
            Assert::same($futures[0]->await()->offset, 0);
            Assert::same($futures[2]->await()->offset, 1);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function failedProducerIdReachesEveryCaller(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(30))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(idempotent: true));

            $proxy->disconnect();

            $failures = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($producer, $topic, &$failures): void {
                $calls = [
                    async(static function () use ($producer, $topic): void {
                        $producer->produce(new Record(value: 'a', topic: $topic));
                    }),
                    async(static function () use ($producer, $topic): void {
                        $producer->produce(new Record(value: 'b', topic: $topic));
                    }),
                ];

                foreach ($calls as $call) {
                    try {
                        $call->await();
                        $failures[] = null;
                    } catch (\Throwable $e) {
                        $failures[] = $e instanceof NetworkException;
                    }
                }

                // One caller alone gets the error too.
                try {
                    $producer->produce(new Record(value: 'c', topic: $topic));
                    $failures[] = null;
                } catch (\Throwable $e) {
                    $failures[] = $e instanceof NetworkException;
                }

                // An unhandled error of the shared request reaches the event loop later.
                delay(0.1);
            });

            Assert::same(self::describe($errors), []);
            Assert::same($failures, [true, true, true]);

            $proxy->reconnect();

            // The second batch uses the same producer id, so the broker accepts its sequence.
            $first = $producer->produceSync(new Record(value: 'd', topic: $topic));
            $second = $producer->produceSync(new Record(value: 'e', topic: $topic));
            Assert::same([$first->topic, $first->partition, $first->offset], [$topic, 0, 0]);
            Assert::same([$second->topic, $second->partition, $second->offset], [$topic, 0, 1]);
        });
    }

    #[Test]
    public function bulkProduceThrowsCancellationOfCall(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $known = $kafka->topic();
            $new = $kafka->topic();
            $producer = $kafka->producer(new Producer\Config(
                batchSize: 400,
                manualFlush: true,
            ));

            // The metadata of this topic is in the cache now.
            $producer->produce(new Record(value: 'warm', topic: $known));
            $producer->flush();

            $cancelled = new DeferredCancellation();
            $cancelled->cancel();

            $failure = null;

            $errors = $kafka->loopErrorsDuring(static function () use ($producer, $known, $new, $cancelled, &$failure): void {
                try {
                    // The full batch does not stop the call. The wait for the metadata of the new topic does.
                    $producer->produce([
                        new Record(value: str_repeat('a', 180), topic: $known),
                        new Record(value: str_repeat('b', 180), topic: $known),
                        new Record(value: 'c', topic: $new),
                    ], $cancelled->getCancellation());
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                // The futures of the call are gone. Their errors must not reach the event loop.
                gc_collect_cycles();
                delay(0.1);
            });

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same(self::describe($errors), []);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceThatWaitsForQueueFailsAtClose(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(
                linger: TimeSpan::fromSeconds(3),
                produceAttempts: 3,
                batchSize: 256,
            ));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            $errors = $kafka->loopErrorsDuring(static function () use ($kafka, $log, $producer, $topic): void {
                // Two records do not fit into one batch. So each record releases the batch before it.
                // The first batch is in flight, the next ones wait in the queues of the sender,
                // until a call waits to push its full batch.
                $calls = [];

                for ($i = 0; $i < 10; ++$i) {
                    // A future cannot be the result of async(), so the call gives a list.
                    $call = async(static fn() => [$producer->produce(new Record(value: str_repeat((string) $i, 100), topic: $topic))]);
                    $calls[] = $call;
                    delay(0.05);

                    if (!$call->isComplete()) {
                        break;
                    }
                }

                $log->waitForAttempts($kafka, count: 2);

                $waiting = array_pop($calls);
                \assert($waiting !== null);
                Assert::false($waiting->isComplete());

                $producer->close(new TimeoutCancellation(0.2));

                $failure = null;

                try {
                    $waiting->await();
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                Assert::instanceOf($failure, ProducerClosed::class);

                // The records of the other calls are in batches. They fail with the cancellation.
                $futures = [];
                foreach ($calls as $call) {
                    /** @var array{Future<Produced>} $result */
                    $result = $call->await();
                    $futures[] = $result[0];
                }

                $failures = self::failures($futures);
                Assert::int(\count($failures))->greaterThanOrEqual(2);
                Assert::same(array_unique($failures), [CancelledException::class]);
                Assert::same($log->attempts(), [1, 1]);

                // A batch of the last record would be released by its linger timer.
                delay(0.2);
            });

            Assert::same(self::describe($errors), []);
        });
    }

    /**
     * @param list<\Throwable> $errors
     * @return list<string> class and message of each error
     */
    private static function describe(array $errors): array
    {
        return array_map(static fn(\Throwable $error) => \sprintf('%s: %s', $error::class, $error->getMessage()), $errors);
    }

    /**
     * @param array<Future<Produced>> $futures
     * @return list<?class-string<\Throwable>> the exception class of each future, null when it has a result
     */
    private static function failures(array $futures): array
    {
        $failures = [];

        foreach ($futures as $future) {
            try {
                $future->await();
                $failures[] = null;
            } catch (\Throwable $e) {
                $failures[] = $e::class;
            }
        }

        return $failures;
    }
}
