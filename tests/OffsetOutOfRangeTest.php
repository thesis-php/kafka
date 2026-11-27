<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\TimeoutCancellation;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Exception\OffsetOutOfRange;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class OffsetOutOfRangeTest
{
    /**
     * The topics of these tests have 3 records, so this offset is behind the end of the log.
     */
    private const int BEHIND_END = 100;

    #[Test]
    public function earliestReadsFromFirstRecord(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            [$topic, $groupId] = self::committedBehindEnd($kafka);

            $consumer = $kafka->groupConsumer($topic, groupId: $groupId, autoOffsetReset: AutoOffsetReset::Earliest);
            $records = $kafka->consume($consumer, count: 3);

            Assert::same(self::describe($records), ['0:a', '1:b', '2:c']);
        });
    }

    #[Test]
    public function earliestResetDoesNotSpin(): void
    {
        $log = new ProduceLog();

        Kafka::connect()->logger($log->logger())->test(static function (Kafka $kafka) use ($log): void {
            [$topic, $groupId] = self::committedBehindEnd($kafka);

            $consumer = $kafka->groupConsumer($topic, groupId: $groupId, autoOffsetReset: AutoOffsetReset::Earliest);
            Assert::count($kafka->consume($consumer, count: 3), 3);

            // One fetch is out of range. The reset fixes it, so no other fetch fails.
            $errors = array_filter(
                [...$log->records('offset out of range, resetting'), ...$log->records('partition fetch failed, retrying')],
                static fn(LogRecord $record) => ($record->context['topic'] ?? null) === $topic,
            );

            Assert::same(array_map(static fn(LogRecord $record) => $record->context['offset'] ?? null, $errors), [self::BEHIND_END]);
        });
    }

    #[Test]
    public function latestReadsOnlyNewRecords(): void
    {
        $log = new ProduceLog();

        Kafka::connect()->logger($log->logger())->test(static function (Kafka $kafka) use ($log): void {
            [$topic, $groupId] = self::committedBehindEnd($kafka);

            $consumer = $kafka->groupConsumer($topic, groupId: $groupId, autoOffsetReset: AutoOffsetReset::Latest);

            // The first call starts the consumer. It reads nothing: the reset goes to the end of the log.
            Assert::same(iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(0.5)), false), []);

            $kafka
                ->poll(static fn(Cancellation $_) => $log->records('offset reset'))
                ->expecting('a reset of the offset in the log')
                ->until(static fn(array $records) => $records !== []);

            $kafka->producer()->produceSync(new Producer\Record(value: 'new', topic: $topic, partition: 0));

            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['3:new']);
        });
    }

    #[Test]
    public function noneThrowsOffsetOutOfRange(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            [$topic, $groupId] = self::committedBehindEnd($kafka);

            $consumer = $kafka->groupConsumer($topic, groupId: $groupId, autoOffsetReset: AutoOffsetReset::None);

            $failure = null;

            try {
                foreach ($consumer->consume(cancellation: new TimeoutCancellation(10)) as $_) {
                    Assert::fail('The consumer must not read records.');
                }
            } catch (OffsetOutOfRange $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, OffsetOutOfRange::class);
            Assert::same([$failure->partition->topic, $failure->partition->partition, $failure->offset], [$topic, 0, self::BEHIND_END]);
        });
    }

    #[Test]
    public function startOfLogMovesPastPositionWhileReading(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            // One batch per record. With the smallest partition limit a fetch gets one batch.
            // Without a buffer the fetcher holds at most one batch when the records are deleted.
            foreach (['a', 'b', 'c', 'd', 'e'] as $value) {
                $producer->produceSync(new Producer\Record(value: $value, topic: $topic, partition: 0));
            }

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                maxPartitionBytes: 1,
                autoOffsetReset: AutoOffsetReset::Earliest,
                prefetchBufferSize: 0,
            ));

            $first = $kafka->consume($consumer, count: 1);
            Assert::same(self::describe($first), ['0:a']);

            $kafka->deleteRecords($topic, 0, 3);

            $rest = $kafka->consume($consumer, count: 2);
            $last = end($rest);
            if ($last !== false && $last->offset !== 4) {
                $rest = [...$rest, ...$kafka->consume($consumer, count: 1)];
            }

            // The fetcher can hold record 1 already when the records are deleted.
            // Record 2 is gone, the consumer goes on from the new start of the log.
            Assert::same(
                array_values(array_diff(self::describe($rest), ['1:b'])),
                ['3:d', '4:e'],
            );
        });
    }

    /**
     * Creates a topic with 3 records and a group whose committed offset is behind the end of the log.
     *
     * @return array{non-empty-string, non-empty-string} the topic and the group id
     */
    private static function committedBehindEnd(Kafka $kafka): array
    {
        $topic = $kafka->topic();

        $kafka->producer()->produceSync([
            new Producer\Record(value: 'a', topic: $topic, partition: 0),
            new Producer\Record(value: 'b', topic: $topic, partition: 0),
            new Producer\Record(value: 'c', topic: $topic, partition: 0),
        ]);

        $groupId = $kafka->groupId();
        $kafka->commitOffset($groupId, $topic, 0, self::BEHIND_END);

        return [$topic, $groupId];
    }

    /**
     * @param list<Consumer\Record> $records
     * @return list<string> the offset and the value of each record
     */
    private static function describe(array $records): array
    {
        return array_map(static fn(Consumer\Record $record) => "{$record->offset}:{$record->value}", $records);
    }
}
