<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Testo\Assert;
use Testo\Data\DataProvider;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\ProduceTimedOut;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Kafka\Testing\Setup;
use Thesis\Time\TimeSpan;
use function Amp\delay;

final readonly class IdempotentProducerTest
{
    /**
     * The topic takes only small records, so a large record fails on the broker.
     */
    private const array SMALL_RECORDS = ['max.message.bytes' => '1024'];

    #[Test]
    #[DataProvider([Kafka::class, 'versions'])]
    public function recordAfterFailedBatchIsDelivered(Setup $setup): void
    {
        $setup->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(configs: self::SMALL_RECORDS);
            $producer = $kafka->producer(new Producer\Config(idempotent: true));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            self::assertTooLarge(self::failure($producer->produce(self::large($topic))));

            Assert::same($producer->produceSync(new Record(value: 'second', topic: $topic))->offset, 1);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 2);

            Assert::same(self::values($records), ['first', 'second']);
        });
    }

    #[Test]
    public function recordsAfterEachFailedBatchAreDelivered(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(configs: self::SMALL_RECORDS);
            $producer = $kafka->producer(new Producer\Config(idempotent: true));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);
            self::assertTooLarge(self::failure($producer->produce(self::large($topic))));
            Assert::same($producer->produceSync(new Record(value: 'second', topic: $topic))->offset, 1);
            self::assertTooLarge(self::failure($producer->produce(self::large($topic))));
            Assert::same($producer->produceSync(new Record(value: 'third', topic: $topic))->offset, 2);
            Assert::same($producer->produceSync(new Record(value: 'fourth', topic: $topic))->offset, 3);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 4);

            Assert::same(self::values($records), ['first', 'second', 'third', 'fourth']);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function batchesBehindFailedBatchAreDeliveredInOrder(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic(configs: self::SMALL_RECORDS);
            [$proxy, $producer] = $kafka->proxiedProducer(new Producer\Config(
                linger: TimeSpan::fromMilliseconds(20),
                produceAttempts: 3,
                idempotent: true,
            ));

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $proxy->stall();

            // The linger releases a batch for each record. The large one is in flight, the others wait.
            $large = $producer->produce(self::large($topic));
            delay(0.1);

            $futures = [];
            for ($i = 0; $i < 3; ++$i) {
                $futures[] = $producer->produce(new Record(value: "v{$i}", topic: $topic));
                delay(0.1);
            }

            Assert::same($log->attempts(), [1, 1]);

            // The large batch gets a new connection and the answer of the broker.
            $proxy->drop();
            $producer->flush();

            self::assertTooLarge(self::failure($large));
            Assert::same(self::offsets($futures), [1, 2, 3]);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 4);

            Assert::same(self::values($records), ['first', 'v0', 'v1', 'v2']);
        });
    }

    #[Test]
    public function failedBatchDoesNotDisturbAnotherPartition(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2, configs: self::SMALL_RECORDS);
            $producer = $kafka->producer(new Producer\Config(idempotent: true));

            Assert::same($producer->produceSync(new Record(value: 'a0', topic: $topic, partition: 1))->offset, 0);
            Assert::same($producer->produceSync(new Record(value: 'b0', topic: $topic, partition: 0))->offset, 0);

            // The batch of partition 1 is in flight while the batch of partition 0 fails.
            $other = $producer->produce(new Record(value: 'a1', topic: $topic, partition: 1));
            $large = $producer->produce(self::large($topic, partition: 0));

            self::assertTooLarge(self::failure($large));
            Assert::same($other->await()->offset, 1);

            Assert::same($producer->produceSync(new Record(value: 'a2', topic: $topic, partition: 1))->offset, 2);
            Assert::same($producer->produceSync(new Record(value: 'a3', topic: $topic, partition: 1))->offset, 3);
            Assert::same($producer->produceSync(new Record(value: 'b1', topic: $topic, partition: 0))->offset, 1);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 6);
            $partitions = [];
            foreach ($records as $record) {
                $partitions[$record->partition][] = [$record->offset, $record->value];
            }
            ksort($partitions);

            Assert::same($partitions, [
                0 => [[0, 'b0'], [1, 'b1']],
                1 => [[0, 'a0'], [1, 'a1'], [2, 'a2'], [3, 'a3']],
            ]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function recordAfterTimeoutWithoutWriteIsDelivered(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromMilliseconds(500),
                    produceAttempts: 2,
                    idempotent: true,
                ),
                requestTimeout: TimeSpan::fromMilliseconds(500),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The broker gets no attempt of this record.
            $proxy->silence();
            Assert::instanceOf(self::failure($producer->produce(new Record(value: 'lost', topic: $topic))), ProduceTimedOut::class);
            $proxy->endSilence();

            Assert::same($producer->produceSync(new Record(value: 'second', topic: $topic))->offset, 1);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 2);

            Assert::same(self::values($records), ['first', 'second']);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function recordAfterTimeoutWithWriteIsDelivered(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromMilliseconds(500),
                    produceAttempts: 2,
                    idempotent: true,
                ),
                requestTimeout: TimeSpan::fromMilliseconds(500),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The broker writes the record, but every answer comes after the end of the attempt.
            $proxy->delayAnswers(1.5);
            Assert::instanceOf(self::failure($producer->produce(new Record(value: 'late', topic: $topic))), ProduceTimedOut::class);
            $proxy->delayAnswers(0);

            // The broker has the record of the failed future once, so the next record gets offset 2.
            Assert::same($producer->produceSync(new Record(value: 'second', topic: $topic))->offset, 2);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 3);

            Assert::same(self::values($records), ['first', 'late', 'second']);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function retriedBatchIsNotWrittenTwice(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(30))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromSeconds(2),
                    produceAttempts: 3,
                    idempotent: true,
                ),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The answer to the first attempt comes after the end of the attempt.
            $proxy->delayAnswers(4);
            $future = $producer->produce(new Record(value: 'second', topic: $topic));

            // The consumer does not use the proxy. It sees that the broker wrote the record.
            $consumer = $kafka->directConsumer($topic, new Consumer\Config(pollTimeout: TimeSpan::fromMilliseconds(100)));
            Assert::same(self::values($kafka->consume($consumer, count: 2)), ['first', 'second']);

            // The next attempt gets its answer in time.
            $proxy->delayAnswers(0);

            Assert::same($future->await()->offset, 1);
            Assert::same($log->attempts(), [1, 1, 2]);
            Assert::same($producer->produceSync(new Record(value: 'third', topic: $topic))->offset, 2);

            $records = $kafka->consume($kafka->groupConsumer($topic), count: 3);

            Assert::same(self::values($records), ['first', 'second', 'third']);
        });
    }

    /**
     * @param non-empty-string $topic
     * @param ?non-negative-int $partition
     */
    private static function large(string $topic, ?int $partition = null): Record
    {
        return new Record(value: str_repeat('x', 4_096), topic: $topic, partition: $partition);
    }

    /**
     * @param Future<Produced> $future
     */
    private static function failure(Future $future): ?\Throwable
    {
        try {
            $future->await();
        } catch (\Throwable $e) {
            return $e;
        }

        return null;
    }

    private static function assertTooLarge(?\Throwable $failure): void
    {
        Assert::instanceOf($failure, ProtocolError::class);
        Assert::same(
            array_map(static fn(Error $error) => $error->code(), $failure->errors->errors),
            [ErrorCode::MessageTooLarge],
        );
    }

    /**
     * @param list<Future<Produced>> $futures
     * @return list<?int>
     */
    private static function offsets(array $futures): array
    {
        return array_map(static fn(Future $future) => $future->await()->offset, $futures);
    }

    /**
     * @param list<Consumer\Record> $records
     * @return list<?string>
     */
    private static function values(array $records): array
    {
        return array_map(static fn(Consumer\Record $record) => $record->value, $records);
    }
}
