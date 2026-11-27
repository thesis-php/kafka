<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Time\TimeSpan;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class FetchRecoveryTest
{
    #[Test]
    public function directConsumerFollowsMovedLeader(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            self::readAcrossLeaderMove($kafka, static fn(string $topic) => $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            )));
        });
    }

    #[Test]
    public function groupConsumerFollowsMovedLeader(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            self::readAcrossLeaderMove($kafka, static fn(string $topic) => $kafka->groupConsumer($topic));
        });
    }

    #[Test]
    public function otherPartitionGoesOnWhilePartitionWaits(): void
    {
        $waiting = new DeferredFuture();
        $release = new DeferredFuture();
        $topic = null;

        // The log record of the failed partition waits for the release. So the
        // fetch of this partition waits too, as long as the test wants.
        $log = new ProduceLog();
        $logger = $log->logger(static function (LogRecord $record) use (&$topic, $waiting, $release): LogRecord {
            if ($record->message === 'partition fetch failed, retrying'
                && ($record->context['topic'] ?? null) === $topic
                && ($record->context['partition'] ?? null) === 0
                && !$waiting->isComplete()) {
                $waiting->complete();

                try {
                    $release->getFuture()->await(new TimeoutCancellation(20));
                } catch (CancelledException) {
                    // The scenario has failed. Do not hold the consumer at its close.
                }
            }

            return $record;
        });

        Kafka::connect()->logger($logger)->test(static function (Kafka $kafka) use (&$topic, $waiting, $release): void {
            $topic = $kafka->topic(partitions: 2, replicas: 2);

            $leaders = [];
            foreach ($kafka->waitUntilTopicReady($topic, 2)->topics[0]->partitions ?? [] as $partition) {
                $leaders[$partition->partitionIndex] = $partition->leaderId;
            }
            Assert::notSame($leaders[0] ?? null, $leaders[1] ?? null, 'The partitions need different leaders.');

            $producer = $kafka->producer();
            $producer->produceSync([
                new Producer\Record(value: 'a0', topic: $topic, partition: 0),
                new Producer\Record(value: 'b0', topic: $topic, partition: 1),
            ]);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest));
            Assert::count($kafka->consume($consumer, count: 2), 2);

            // The old leader rejects the next fetch of partition 0.
            $kafka->moveLeader($topic, 0);
            $kafka
                ->poll(static fn(Cancellation $_) => $waiting->isComplete())
                ->expecting('a failed fetch of partition 0')
                ->until(static fn(bool $failed) => $failed);

            $producer->produceSync([
                new Producer\Record(value: 'a1', topic: $topic, partition: 0),
                new Producer\Record(value: 'b1', topic: $topic, partition: 1),
            ]);

            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['1:1:b1']);

            $release->complete();

            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['0:1:a1']);
        });
    }

    #[Test]
    public function silentConnectionIsReplaced(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(30))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            [$proxy, $consumer] = $kafka->proxiedConsumer(
                $topic,
                new Consumer\Config(maxWait: TimeSpan::fromMilliseconds(500), autoOffsetReset: AutoOffsetReset::Earliest),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            $producer->produceSync(new Producer\Record(value: 'first', topic: $topic));
            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['0:0:first']);

            $accepted = $proxy->accepted();

            // The open connections stay open and never answer.
            $proxy->stall();
            $producer->produceSync(new Producer\Record(value: 'second', topic: $topic));

            Assert::same(self::describe($kafka->consume($consumer, count: 1, timeout: TimeSpan::fromSeconds(15))), ['0:1:second']);
            Assert::int($proxy->accepted())->greaterThan($accepted);
        });
    }

    #[Test]
    public function goneBrokerIsRetriedWithPauses(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(30))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            [$proxy, $consumer] = $kafka->proxiedConsumer(
                $topic,
                new Consumer\Config(maxWait: TimeSpan::fromMilliseconds(500), autoOffsetReset: AutoOffsetReset::Earliest),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            $producer->produceSync(new Producer\Record(value: 'first', topic: $topic));
            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['0:0:first']);

            $proxy->disconnect();

            $attempts = $kafka
                ->poll(static fn(Cancellation $_) => $log->records('fetch recovery failed, retrying'))
                ->expecting('5 failed attempts of the fetch recovery')
                ->describedBy(static fn(array $records) => \sprintf('%d attempts', \count($records)))
                ->until(static fn(array $records) => \count($records) >= 5);

            $proxy->reconnect();

            // Each attempt waits for its backoff, at least 50 ms.
            $times = array_map(static fn(LogRecord $record) => (float) $record->datetime->format('U.u'), $attempts);
            $pauses = array_map(static fn(float $before, float $after) => $after - $before, \array_slice($times, 0, -1), \array_slice($times, 1));

            foreach ($pauses as $i => $pause) {
                Assert::true($pause >= 0.045, \sprintf('Attempt %d came %.3f s after the previous one.', $i + 2, $pause));
            }

            $producer->produceSync(new Producer\Record(value: 'second', topic: $topic));
            Assert::same(self::describe($kafka->consume($consumer, count: 1, timeout: TimeSpan::fromSeconds(15))), ['0:1:second']);
        });
    }

    #[Test]
    public function deletedTopicDoesNotStopOtherTopic(): void
    {
        $log = new ProduceLog();

        Kafka::connect()->logger($log->logger())->test(static function (Kafka $kafka) use ($log): void {
            $kept = $kafka->topic();
            $deleted = $kafka->topic();
            $producer = $kafka->producer();

            // A short metadata age: the client soon forgets the deleted topic.
            $client = $kafka->createClient(metadataMaxAge: TimeSpan::fromMilliseconds(500));
            $consumer = $kafka->directConsumer([$kept, $deleted], new Consumer\Config(
                maxWait: TimeSpan::fromMilliseconds(500),
                autoOffsetReset: AutoOffsetReset::Earliest,
            ), client: $client);

            $producer->produceSync([
                new Producer\Record(value: 'kept-0', topic: $kept),
                new Producer\Record(value: 'deleted-0', topic: $deleted),
            ]);
            Assert::count($kafka->consume($consumer, count: 2), 2);

            $kafka->deleteTopic($deleted);

            $producer->produceSync(new Producer\Record(value: 'kept-1', topic: $kept));
            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['0:1:kept-1']);

            // The partition of the deleted topic still waits after some attempts.
            $kafka
                ->poll(static fn(Cancellation $_) => $log->records('partitions wait for a leader'))
                ->expecting('3 attempts to find the deleted topic')
                ->until(static fn(array $records) => \count($records) >= 3);

            $producer->produceSync(new Producer\Record(value: 'kept-2', topic: $kept));
            Assert::same(self::describe($kafka->consume($consumer, count: 1)), ['0:2:kept-2']);
        });
    }

    /**
     * @param \Closure(non-empty-string): Consumer $create
     */
    private static function readAcrossLeaderMove(Kafka $kafka, \Closure $create): void
    {
        $topic = $kafka->topic(replicas: 3);
        $producer = $kafka->producer();

        $producer->produceSync([
            new Producer\Record(value: 'r0', topic: $topic, partition: 0),
            new Producer\Record(value: 'r1', topic: $topic, partition: 0),
            new Producer\Record(value: 'r2', topic: $topic, partition: 0),
        ]);

        $consumer = $create($topic);
        $before = $kafka->consume($consumer, count: 3);

        $kafka->moveLeader($topic);

        $producer->produceSync([
            new Producer\Record(value: 'r3', topic: $topic, partition: 0),
            new Producer\Record(value: 'r4', topic: $topic, partition: 0),
            new Producer\Record(value: 'r5', topic: $topic, partition: 0),
        ]);

        $after = $kafka->consume($consumer, count: 3);

        Assert::same(
            self::describe([...$before, ...$after]),
            ['0:0:r0', '0:1:r1', '0:2:r2', '0:3:r3', '0:4:r4', '0:5:r5'],
        );
    }

    /**
     * @param list<Consumer\Record> $records
     * @return list<string> the partition, the offset and the value of each record
     */
    private static function describe(array $records): array
    {
        return array_map(static fn(Consumer\Record $record) => "{$record->partition}:{$record->offset}:{$record->value}", $records);
    }
}
