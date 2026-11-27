<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Producer\Partitioner\RoundRobinPartitioner;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

final readonly class ProducerConcurrencyTest
{
    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function concurrentProduceToOnePartitionLosesNoRecord(): void
    {
        self::concurrentProduceToOnePartition(idempotent: false);
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function concurrentIdempotentProduceToOnePartitionLosesNoRecord(): void
    {
        self::concurrentProduceToOnePartition(idempotent: true);
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function recordThatFillsBatchAloneWaitsForSlowPartition(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(self::fullBatchConfig());

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            // Each record is larger than a batch, so each call releases its own batch at once.
            [$calls, $waiting] = self::produceUntilCallWaits($producer, $topic, partition: 0, size: 300);

            // One batch is in flight, and one batch waits for it. The next call waits.
            Assert::count($calls, 2);

            $proxy->drop();
            $calls[] = $waiting;
            $futures = self::futures($calls);
            $producer->flush();

            Assert::same(self::partitionOffsets($futures), [[0, 1], [0, 2], [0, 3]]);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 4);

            Assert::same(self::values($records), ['first', str_repeat('0', 300), str_repeat('1', 300), str_repeat('2', 300)]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function slowPartitionDoesNotBlockBatchesOfAnother(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic(partitions: 2);
            [$proxy, $producer] = $kafka->proxiedProducer(self::fullBatchConfig());

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic, partition: 0))->offset, 0);

            $proxy->stall();

            [$calls, $waiting] = self::produceUntilCallWaits($producer, $topic, partition: 0);
            $log->waitForAttempts($kafka, count: 2);

            // The record fills a batch at once, so the batch is released without a linger.
            $other = $producer->produce(new Record(value: str_repeat('o', 300), topic: $topic, partition: 1));

            $kafka
                ->poll(static fn(Cancellation $_) => self::attemptPartitions($log))
                ->expecting('a produce attempt for partition 1')
                ->describedBy(static fn(array $partitions) => json_encode($partitions, JSON_THROW_ON_ERROR))
                ->until(static fn(array $partitions) => \in_array(1, $partitions, true));

            // The batches of partition 0 still wait behind the batch in flight.
            Assert::false($waiting->isComplete());
            Assert::same(self::attemptPartitions($log), [0, 0, 1]);

            $proxy->drop();
            $calls[] = $waiting;
            $futures = self::futures($calls);
            $producer->flush();

            Assert::same(self::partitionOffsets($futures), [[0, 1], [0, 2], [0, 3]]);
            Assert::same(self::partitionOffsets([$other]), [[1, 0]]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function slowPartitionDoesNotBlockProduceToAnother(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2);
            [$proxy, $producer] = $kafka->proxiedProducer(self::fullBatchConfig());

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic, partition: 0))->offset, 0);

            $proxy->stall();

            [$calls, $waiting] = self::produceUntilCallWaits($producer, $topic, partition: 0);

            // Two records do not fit into one batch, so the second call releases a full batch of partition 1.
            $others = [];
            for ($i = 0; $i < 2; ++$i) {
                $other = async(static fn() => [$producer->produce(new Record(value: str_repeat((string) $i, 100), topic: $topic, partition: 1))]);
                delay(0.05);

                Assert::true($other->isComplete());
                $others[] = $other;
            }

            Assert::false($waiting->isComplete());

            $proxy->drop();
            $calls[] = $waiting;
            $futures = self::futures($calls);
            $otherFutures = self::futures($others);
            $producer->flush();

            Assert::same(self::partitionOffsets($futures), [[0, 1], [0, 2], [0, 3]]);
            Assert::same(self::partitionOffsets($otherFutures), [[1, 0], [1, 1]]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function batchesOfPartitionKeepOrder(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(20),
                produceAttempts: 3,
            ));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            // The linger releases a batch for each record. The first one is in flight, the others wait.
            $values = [];
            for ($i = 0; $i < 5; ++$i) {
                $values[] = "v{$i}";
                $producer->produce(new Record(value: "v{$i}", topic: $topic));
                delay(0.1);
            }

            Assert::same($log->attempts(), [1, 1]);

            $proxy->drop();
            $producer->flush();

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 6);

            Assert::same(
                array_map(static fn(Consumer\Record $record) => $record->value, $records),
                ['first', ...$values],
            );
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function cancelledWaitDoesNotQueueRecord(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(self::fullBatchConfig());

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            $cancellation = new DeferredCancellation();
            [$calls, $waiting] = self::produceUntilCallWaits($producer, $topic, partition: 0, cancellation: $cancellation->getCancellation());

            $cancellation->cancel();

            $failure = null;

            try {
                $waiting->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, CancelledException::class);

            $proxy->drop();
            $futures = self::futures($calls);
            $producer->flush();

            Assert::same(self::partitionOffsets($futures), [[0, 1], [0, 2]]);

            // The record of the cancelled call is not in the topic, so the next record gets the next offset.
            Assert::same($producer->produceSync(new Record(value: 'last', topic: $topic))->offset, 3);
        });
    }

    #[Test]
    public function fullBatchWithFreePartitionIgnoresCancellation(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer(self::fullBatchConfig());

            // The metadata of the topic is in the cache now.
            Assert::same($producer->produceSync(new Record(value: 'warm', topic: $topic))->offset, 0);

            $cancelled = new DeferredCancellation();
            $cancelled->cancel();

            // The second record releases the batch of the first one. The partition takes it at once, so nothing waits.
            $futures = [
                $producer->produce(new Record(value: str_repeat('a', 100), topic: $topic), $cancelled->getCancellation()),
                $producer->produce(new Record(value: str_repeat('b', 100), topic: $topic), $cancelled->getCancellation()),
            ];
            $producer->flush();

            Assert::same(self::partitionOffsets($futures), [[0, 1], [0, 2]]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceSyncCancellationEndsWaitForDelivery(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            $failure = null;
            $records = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($kafka, $proxy, $producer, $topic, &$failure, &$records): void {
                try {
                    $producer->produceSync(new Record(value: 'second', topic: $topic), new TimeoutCancellation(0.3));
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                $proxy->drop();
                $records = $kafka->consume($kafka->groupConsumer($topic), count: 2);
            });

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same(self::describe($errors), []);
            Assert::same(self::values($records), ['first', 'second']);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function bulkProduceSyncCancellationEndsWaitForDelivery(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            $failure = null;
            $records = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($kafka, $proxy, $producer, $topic, &$failure, &$records): void {
                try {
                    $producer->produceSync([
                        new Record(value: 'second', topic: $topic),
                        new Record(value: 'third', topic: $topic),
                    ], new TimeoutCancellation(0.3));
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                $proxy->drop();
                $records = $kafka->consume($kafka->groupConsumer($topic), count: 3);
            });

            Assert::instanceOf($failure, CancelledException::class);
            Assert::same(self::describe($errors), []);
            Assert::same(self::values($records), ['first', 'second', 'third']);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function produceSyncCancellationLeavesFailedRecordsHandled(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(produceAttempts: 3));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            $failures = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($producer, $topic, &$failures): void {
                $calls = [
                    static fn() => $producer->produceSync(new Record(value: 'second', topic: $topic), new TimeoutCancellation(0.2)),
                    static fn() => $producer->produceSync([new Record(value: 'third', topic: $topic)], new TimeoutCancellation(0.2)),
                ];

                foreach ($calls as $call) {
                    try {
                        $call();
                        $failures[] = null;
                    } catch (\Throwable $e) {
                        $failures[] = $e::class;
                    }
                }

                // The records fail now. Nobody waits for them any more.
                $producer->close(new TimeoutCancellation(0.2));
                gc_collect_cycles();
                delay(0.1);
            });

            Assert::same($failures, [CancelledException::class, CancelledException::class]);
            Assert::same(self::describe($errors), []);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function lingerOfDeletedTopicFailsRecord(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2);
            $client = $kafka->createClient(metadataMaxAge: TimeSpan::fromMilliseconds(200));
            $producer = $kafka->producer(new Producer\Config(
                linger: TimeSpan::fromSeconds(2),
                partitioner: new RoundRobinPartitioner(),
            ), client: $client);

            $future = $producer->produce(new Record(value: 'lost', topic: $topic));

            $errors = $kafka->loopErrorsDuring(static function () use ($kafka, $topic, $future): void {
                // The periodic refresh drops the deleted topic from the cache before the linger ends.
                $kafka->deleteTopic($topic);

                $kafka
                    ->poll(static fn(Cancellation $_) => $future->isComplete())
                    ->expecting('the record has a result')
                    ->within(TimeSpan::fromSeconds(30))
                    ->until(static fn(bool $complete) => $complete);
            });

            Assert::same(self::describe($errors), []);

            $failure = null;

            try {
                $future->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, UnknownTopic::class);
        });
    }

    private static function concurrentProduceToOnePartition(bool $idempotent): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($idempotent): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(self::fullBatchConfig(idempotent: $idempotent));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            [$calls, $waiting] = self::produceUntilCallWaits($producer, $topic, partition: 0);

            // One batch is in flight, and one full batch waits for it. The next call waits.
            Assert::count($calls, 2);

            // The full batch of the first call still waits for the partition, so this call waits too.
            $second = async(static fn() => [$producer->produce(new Record(value: 'second-call', topic: $topic))]);
            delay(0.05);

            Assert::false($second->isComplete());
            Assert::false($waiting->isComplete());

            // Both calls go on after the drop, and each one uses the batch that is open at that moment.
            $proxy->drop();
            $calls[] = $waiting;
            $calls[] = $second;
            $futures = self::futures($calls);
            $producer->flush();

            $offsets = array_map(static fn(array $offset) => $offset[1], self::partitionOffsets($futures));
            sort($offsets);
            Assert::same($offsets, [1, 2, 3, 4]);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 5);
            $values = self::values($records);
            sort($values);

            $expected = ['first', 'second-call', str_repeat('0', 100), str_repeat('1', 100), str_repeat('2', 100)];
            sort($expected);

            Assert::same($values, $expected);
        });
    }

    /**
     * Two records do not fit into one batch, so each record releases the batch
     * before it. The first batch is in flight behind the stalled proxy.
     */
    private static function fullBatchConfig(bool $idempotent = false): Producer\Config
    {
        return new Producer\Config(
            linger: TimeSpan::fromSeconds(3),
            produceAttempts: 3,
            batchSize: 256,
            idempotent: $idempotent,
        );
    }

    /**
     * Produces records in calls of their own until a call waits.
     *
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param positive-int $size the size of each record value
     * @return array{list<Future<array{Future<Produced>}>>, Future<array{Future<Produced>}>} the calls that returned, and the call that waits
     */
    private static function produceUntilCallWaits(
        Producer $producer,
        string $topic,
        int $partition,
        Cancellation $cancellation = new NullCancellation(),
        int $size = 100,
    ): array {
        $calls = [];

        for ($i = 0; $i < 10; ++$i) {
            // A future cannot be the result of async(), so the call gives a list.
            $call = async(static fn() => [$producer->produce(
                new Record(value: str_repeat((string) $i, $size), topic: $topic, partition: $partition),
                $cancellation,
            )]);
            delay(0.05);

            if (!$call->isComplete()) {
                return [$calls, $call];
            }

            $calls[] = $call;
        }

        throw new \RuntimeException('No call waits.');
    }

    /**
     * @param list<Future<array{Future<Produced>}>> $calls
     * @return list<Future<Produced>>
     */
    private static function futures(array $calls): array
    {
        return array_map(static fn(Future $call) => $call->await()[0], $calls);
    }

    /**
     * @param list<Future<Produced>> $futures
     * @return list<array{int, ?int}> the partition and the offset of each record
     */
    private static function partitionOffsets(array $futures): array
    {
        return array_map(static function (Future $future): array {
            $produced = $future->await();

            return [$produced->partition, $produced->offset];
        }, $futures);
    }

    /**
     * @return list<mixed> the partition of each produce attempt in the log
     */
    private static function attemptPartitions(ProduceLog $log): array
    {
        return array_map(
            static fn(\Monolog\LogRecord $record) => $record->context['partition'] ?? null,
            $log->records('producing batch'),
        );
    }

    /**
     * @param list<Consumer\Record> $records
     * @return list<?string>
     */
    private static function values(array $records): array
    {
        return array_map(static fn(Consumer\Record $record) => $record->value, $records);
    }

    /**
     * @param list<\Throwable> $errors
     * @return list<string> class and message of each error
     */
    private static function describe(array $errors): array
    {
        return array_map(static fn(\Throwable $error) => \sprintf('%s: %s', $error::class, $error->getMessage()), $errors);
    }
}
