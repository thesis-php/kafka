<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\BatchFull;
use Thesis\Kafka\Producer\Partitioner\RoundRobinPartitioner;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Time\TimeSpan;
use function Amp\delay;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class ProducerTest
{
    #[Test]
    public function lingerDelaysFutureResolution(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $startedAt = self::nowMs();
            $producer = $kafka->producer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(300),
            ));
            $future = $producer->produce(new Record(
                value: 'linger-test',
                topic: $topic,
            ));

            $result = $future->await();
            $elapsed = self::nowMs() - $startedAt;

            Assert::int($elapsed)->greaterThanOrEqual(200)->lessThan(1_000);
            Assert::same($result->topic, $topic);
            Assert::same($result->partition, 0);
            Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);
        });
    }

    #[Test]
    public function fullBatchBypassesLingerAndResolvesImmediately(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);

            $startedAt = self::nowMs();

            $producer = $kafka->producer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(3_000),
                batchSize: 256,
                partitioner: new RoundRobinPartitioner(),
            ));

            $results = $producer->produce([
                new Record(
                    value: str_repeat('a', 180),
                    topic: $topic,
                    partition: 2,
                ),
                new Record(
                    value: str_repeat('b', 180),
                    topic: $topic,
                    partition: 2,
                ),
            ]);

            /** @var list<Produced> $results */
            $results = Future\await($results);
            $elapsed = self::nowMs() - $startedAt;

            Assert::int($elapsed)->lessThan(1_000);
            \assert(\count($results) === 2);
            Assert::same($results[0]->topic, $topic);
            Assert::same($results[0]->partition, 2);
            Assert::int($results[0]->offset ?? -1)->greaterThanOrEqual(0);
            Assert::same($results[1]->topic, $topic);
            Assert::same($results[1]->partition, 2);
            Assert::same($results[1]->offset, ($results[0]->offset ?? 0) + 1);
        });
    }

    #[Test]
    public function closeDrainsBufferedRecordsWithoutWaitingForLinger(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $producer = $kafka->producer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(3_000),
            ));

            $future = $producer->produce(new Record(
                value: 'close-drain-test',
                topic: $topic,
            ));

            $startedAt = self::nowMs();
            $producer->close();
            $result = $future->await();
            $elapsed = self::nowMs() - $startedAt;

            Assert::int($elapsed)->lessThan(100);
            Assert::same($result->topic, $topic);
            Assert::same($result->partition, 0);
            Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);
        });
    }

    #[Test]
    public function manualFlushKeepsProduceFuturesPendingUntilExplicitFlush(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);

            $producer = $kafka->producer(new Producer\Config(
                manualFlush: true,
            ));

            $futures = $producer->produce([
                new Record(
                    value: 'manual-flush-0',
                    topic: $topic,
                    partition: 2,
                ),
                new Record(
                    value: 'manual-flush-1',
                    topic: $topic,
                    partition: 2,
                ),
            ]);

            \assert(\count($futures) === 2);

            delay(0.2);

            Assert::false($futures[0]->isComplete());
            Assert::false($futures[1]->isComplete());

            $producer->flush();

            Assert::true($futures[0]->isComplete());
            Assert::true($futures[1]->isComplete());

            /** @var list<Produced> $results */
            $results = array_values(Future\await($futures));

            \assert(\count($results) === 2);

            Assert::same($results[0]->topic, $topic);
            Assert::same($results[1]->topic, $topic);
            Assert::same($results[0]->partition, 2);
            Assert::same($results[1]->partition, 2);
            Assert::int($results[0]->offset ?? -1)->greaterThanOrEqual(0);
            Assert::same($results[1]->offset, ($results[0]->offset ?? 0) + 1);
        });
    }

    #[Test]
    public function manualFlushThrowsBatchFull(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);

            $producer = $kafka->producer(new Producer\Config(
                batchSize: 256,
                manualFlush: true,
            ));

            $first = $producer->produce(new Record(
                value: str_repeat('a', 180),
                topic: $topic,
                partition: 2,
            ));

            try {
                $producer->produce(new Record(
                    value: str_repeat('b', 180),
                    topic: $topic,
                    partition: 2,
                ));

                Assert::fail('Expected BatchFull to be thrown.');
            } catch (BatchFull $e) {
                Assert::same(
                    $e->getMessage(),
                    'Batch is full in manual-flush mode; call flush() and retry the produce().',
                );
            }

            $producer->flush();

            $firstResult = $first->await();

            Assert::same($firstResult->topic, $topic);
            Assert::same($firstResult->partition, 2);
            Assert::same($firstResult->offset, 0);

            $second = $producer->produce(new Record(
                value: 'after-flush',
                topic: $topic,
                partition: 2,
            ));

            $producer->flush();

            $secondResult = $second->await();

            Assert::same($secondResult->topic, $topic);
            Assert::same($secondResult->partition, 2);
            Assert::same($secondResult->offset, 1);
        });
    }

    #[Test]
    public function idempotentProducerPublishesSuccessfully(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 3);

            $producer = $kafka->producer(new Producer\Config(
                idempotent: true,
            ));

            $result = $producer->produceSync(new Record(
                value: 'idempotent-test',
                topic: $topic,
                partition: 2,
            ));

            Assert::same($result->topic, $topic);
            Assert::same($result->partition, 2);
            Assert::same($result->offset, 0);
        });
    }

    /**
     * @return non-negative-int
     */
    private static function nowMs(): int
    {
        /** @var non-negative-int */
        return intdiv((int) hrtime(true), 1_000_000);
    }
}
