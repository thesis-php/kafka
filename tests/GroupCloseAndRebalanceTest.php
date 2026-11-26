<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

final readonly class GroupCloseAndRebalanceTest
{
    /**
     * The only broker of this setup advertises 127.0.0.1:19094 on its second
     * listener. A proxy on this port puts the coordinator behind the proxy.
     */
    private const string PROXIED_BROKER = 'kafka-proxied-standalone:9092';
    private const int PROXIED_PORT = 19094;

    /** A short heartbeat interval makes a member hear of a rebalance soon. */
    private const float HEARTBEAT_SECONDS = 0.3;

    /**
     * A fetch that is cancelled at a rebalance still holds its connection on
     * the broker until its wait ends. A short wait keeps the next fetch fast.
     */
    private const float MAX_WAIT_SECONDS = 0.3;

    #[Test]
    public function closeWithFullBufferReturns(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, partition: 0, count: 20);

            $consumer = $kafka->groupConsumer($topic);
            $records = $kafka->consume($consumer, count: 1);

            Assert::same(self::values($records), ['p0-0']);

            // Gives the session time to fill the buffer with the other batches.
            delay(0.2);

            $elapsed = self::measure($consumer->close(...));

            Assert::true($elapsed < 5.0, \sprintf('close() took %.1f s.', $elapsed));
        });
    }

    #[Test]
    public function closeFromHandlerReturns(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, partition: 0, count: 20);

            /** @var list<list<?string>> $seen */
            $seen = [];

            $subscription = $kafka->subscribe(
                $topic,
                static function (array $records, ConsumerSession $session) use (&$seen): void {
                    $seen[] = self::values($records);

                    // Gives the session time to fill the buffer with the other batches.
                    delay(0.2);
                    $session->close();
                },
                $kafka->groupConfig(),
            );

            $elapsed = self::measure($subscription->join(...));

            Assert::true($elapsed < 10.0, \sprintf('join() took %.1f s.', $elapsed));
            // The batches in the buffer are not given to the handler after close().
            Assert::same($seen, [['p0-0']]);
        });
    }

    #[Test]
    public function noRecordsOfRevokedPartitionAfterRebalance(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2);
            $producer = $kafka->producer();

            // Six batches and a buffer of four: whatever the order of the batches,
            // the buffer of the first member holds batches of both partitions.
            self::produceBatches($producer, $topic, partition: 0, count: 3);
            self::produceBatches($producer, $topic, partition: 1, count: 3);

            $groupId = $kafka->groupId();
            $heartbeat = TimeSpan::fromSeconds(self::HEARTBEAT_SECONDS);
            $maxWait = TimeSpan::fromSeconds(self::MAX_WAIT_SECONDS);
            $rebalanceTimeout = TimeSpan::fromSeconds(2);

            $first = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: $heartbeat, maxWait: $maxWait);
            Assert::count($kafka->consume($first, count: 1), 1);

            // The first member reads nothing now. The second member joins and gets
            // one partition. It reads it only after the first member has rejoined.
            $second = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: $heartbeat, maxWait: $maxWait);
            $secondRecords = $kafka->consume($second, count: 3);
            $secondPartitions = self::partitions($secondRecords);

            Assert::count($secondPartitions, 1);

            $firstRecords = $kafka->consume($first, count: 3);
            $firstPartitions = self::partitions($firstRecords);

            Assert::same(
                array_values(array_intersect($firstPartitions, $secondPartitions)),
                [],
                \sprintf('The first member got %s after partition %d moved.', implode(', ', self::values($firstRecords)), $secondPartitions[0] ?? -1),
            );
            Assert::count($firstPartitions, 1);

            $partition = $firstPartitions[0] ?? -1;
            Assert::same(self::values($firstRecords), ["p{$partition}-0", "p{$partition}-1", "p{$partition}-2"]);
        });
    }

    #[Test]
    public function rebalanceDeliversNoBufferedDuplicates(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(partitions: 2);
            $producer = $kafka->producer();

            self::produceBatches($producer, $topic, partition: 0, count: 20);
            self::produceBatches($producer, $topic, partition: 1, count: 20);

            $groupId = $kafka->groupId();
            $heartbeat = TimeSpan::fromSeconds(self::HEARTBEAT_SECONDS);
            $maxWait = TimeSpan::fromSeconds(self::MAX_WAIT_SECONDS);
            $stop = new DeferredCancellation();

            /** @var list<?string> $first */
            $first = [];
            /** @var list<?string> $second */
            $second = [];

            /** @param \Closure(?string): void $processed */
            $process = static function (Consumer $consumer, float $pause, \Closure $processed) use ($stop): void {
                $cancellation = $stop->getCancellation();

                try {
                    foreach ($consumer->consume(cancellation: $cancellation) as $batch) {
                        foreach ($batch as $record) {
                            $processed($record->value);
                        }

                        $consumer->commitRecords($batch);
                        delay($pause, cancellation: $cancellation);
                    }
                } catch (CancelledException) {
                    // The test has seen every record.
                }
            };

            // The first member is slow, so its buffer is full when the second member joins.
            $firstReader = async($process, $kafka->groupConsumer($topic, $groupId, heartbeatInterval: $heartbeat, maxWait: $maxWait), 0.05, static function (?string $value) use (&$first): void {
                $first[] = $value;
            });

            $kafka->poll(static function (Cancellation $_) use (&$first): bool {
                return \count($first) >= 5;
            })
                ->expecting('5 records processed by the first member')
                ->until(static fn(bool $done) => $done);

            $secondReader = async($process, $kafka->groupConsumer($topic, $groupId, heartbeatInterval: $heartbeat, maxWait: $maxWait), 0.0, static function (?string $value) use (&$second): void {
                $second[] = $value;
            });

            $kafka->poll(static function (Cancellation $_) use (&$first, &$second): int {
                return \count(array_unique([...$first, ...$second]));
            })
                ->expecting('40 different records processed')
                ->until(static fn(int $count) => $count >= 40);

            $stop->cancel();
            Future\await([$firstReader, $secondReader]);

            $all = [...$first, ...$second];
            $duplicates = array_keys(array_filter(array_count_values(array_map(\strval(...), $all)), static fn(int $count) => $count > 1));

            Assert::same($duplicates, [], \sprintf('Processed twice: %s.', implode(', ', $duplicates)));
            Assert::count($all, 40);
            Assert::true($second !== [], 'The second member processed nothing.');
        });
    }

    #[Test]
    public function slowHandlerKeepsItsMembership(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic(partitions: 2);
            $producer = $kafka->producer();

            self::produceBatches($producer, $topic, partition: 0, count: 5);
            self::produceBatches($producer, $topic, partition: 1, count: 5);

            $groupId = $kafka->groupId();
            // The handler (3 s) is much slower than the heartbeat interval and the rebalance timeout.
            $heartbeat = TimeSpan::fromSeconds(self::HEARTBEAT_SECONDS);
            $maxWait = TimeSpan::fromSeconds(self::MAX_WAIT_SECONDS);
            $rebalanceTimeout = TimeSpan::fromSeconds(1.5);
            $busy = new DeferredFuture();
            $calls = 0;

            $kafka->subscribe(
                $topic,
                static function () use (&$calls, $busy): void {
                    ++$calls;

                    if ($calls === 1) {
                        $busy->complete();
                        delay(3);
                    }
                },
                $kafka->groupConfig($groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: $heartbeat, maxWait: $maxWait),
            );

            $busy->getFuture()->await();

            $second = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, heartbeatInterval: $heartbeat, maxWait: $maxWait);
            Assert::count($kafka->consume($second, count: 1), 1);

            $joins = $kafka->poll(static fn(Cancellation $_) => self::joins($log))
                ->expecting('a generation that both members joined')
                ->describedBy(static fn(array $joins) => json_encode($joins, JSON_THROW_ON_ERROR))
                ->until(static fn(array $joins) => array_filter($joins, static fn(array $members) => \count($members) === 2) !== []);

            $kafka->poll(static function (Cancellation $_) use (&$calls): bool {
                return $calls >= 2;
            })
                ->expecting('the handler called again after the slow batch')
                ->until(static fn(bool $done) => $done);

            $firstMember = $joins[1][0] ?? null;
            $both = array_values(array_filter($joins, static fn(array $members) => \count($members) === 2))[0] ?? [];

            Assert::true($firstMember !== null, 'The first member did not join.');
            Assert::true(
                \in_array($firstMember, $both, true),
                \sprintf('The slow member %s was removed. Members joined: %s.', $firstMember, json_encode($joins, JSON_THROW_ON_ERROR)),
            );
        });
    }

    #[Test]
    public function closeWhenCoordinatorDoesNotAnswer(): void
    {
        $log = new TestHandler();

        Kafka::connect(self::PROXIED_BROKER)->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, partition: 0, count: 1);

            $proxy = $kafka->proxy(new Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
            // The request timeout of the client stays 30 s. The session timeout is shorter.
            $client = $kafka->createClient(seeds: [$proxy->endpoint]);
            $consumer = $kafka->groupConsumer(
                $topic,
                client: $client,
                sessionTimeout: TimeSpan::fromSeconds(2),
                heartbeatInterval: TimeSpan::fromSeconds(self::HEARTBEAT_SECONDS),
            );

            Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);

            // Every connection of the client is open now and goes through the proxy.
            $proxy->stall();

            $elapsed = self::measure($consumer->close(...));

            // The session timeout (2 s) is the limit of the leave.
            Assert::true($elapsed < 5.0, \sprintf('close() took %.1f s.', $elapsed));
            Assert::same(
                array_values(array_map(
                    static fn(LogRecord $record) => $record->message,
                    array_filter($log->getRecords(), static fn(LogRecord $record) => $record->level === Level::Warning && $record->message === 'leave group failed'),
                )),
                ['leave group failed'],
            );
        });
    }

    #[Test]
    public function silentCoordinatorConnectionDoesNotBlockHeartbeat(): void
    {
        $log = new TestHandler();

        Kafka::connect(self::PROXIED_BROKER)->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic(partitions: 2);
            self::produceBatches($kafka->producer(), $topic, partition: 0, count: 1);

            $proxy = $kafka->proxy(new Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
            $client = $kafka->createClient(seeds: [$proxy->endpoint], requestTimeout: TimeSpan::fromSeconds(1));
            $groupId = $kafka->groupId();
            // Short timeouts make a JoinGroup on the silent connection end sooner.
            $rebalanceTimeout = TimeSpan::fromSeconds(1);
            $sessionTimeout = TimeSpan::fromSeconds(3);
            $heartbeat = TimeSpan::fromSeconds(self::HEARTBEAT_SECONDS);
            $first = $kafka->groupConsumer($topic, $groupId, client: $client, rebalanceTimeout: $rebalanceTimeout, sessionTimeout: $sessionTimeout, heartbeatInterval: $heartbeat);

            Assert::same(self::values($kafka->consume($first, count: 1)), ['p0-0']);

            // The open connections stay open and never answer. New connections work.
            $proxy->stall();

            $kafka->poll(static fn(Cancellation $_) => array_values(array_filter(
                $log->getRecords(),
                static fn(LogRecord $record) => $record->message === 'connection does not answer, closing it'
                    && ($record->context['apiKey'] ?? null) === 'Heartbeat',
            )))
                ->expecting('a heartbeat that ran out of time')
                ->until(static fn(array $records) => $records !== []);

            // The second member joins directly. The first member hears of the rebalance only
            // when its heartbeat works again. Its JoinGroup can go to the silent connection
            // of the first join, so it may come back with a new member id.
            $second = $kafka->groupConsumer($topic, $groupId, rebalanceTimeout: $rebalanceTimeout, sessionTimeout: $sessionTimeout, heartbeatInterval: $heartbeat);
            $kafka->consume($second, count: 1);

            $joins = $kafka->poll(static fn(Cancellation $_) => self::joins($log))
                ->expecting('a generation that both members joined')
                ->describedBy(static fn(array $joins) => json_encode($joins, JSON_THROW_ON_ERROR))
                ->until(static fn(array $joins) => array_filter($joins, static fn(array $members) => \count($members) === 2) !== []);

            $both = array_values(array_filter($joins, static fn(array $members) => \count($members) === 2))[0] ?? [];

            Assert::count(array_unique($both), 2);
        });
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param positive-int $count
     */
    private static function produceBatches(Producer $producer, string $topic, int $partition, int $count): void
    {
        // One produce call makes one record batch.
        for ($i = 0; $i < $count; ++$i) {
            $producer->produceSync(new Producer\Record(value: "p{$partition}-{$i}", topic: $topic, partition: $partition));
        }
    }

    /**
     * @param \Closure(): void $step
     * @return float seconds
     */
    private static function measure(\Closure $step): float
    {
        $started = hrtime(true);
        $step();

        return (hrtime(true) - $started) / 1e9;
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
     * @param list<Consumer\Record> $records
     * @return list<int>
     */
    private static function partitions(array $records): array
    {
        return array_values(array_unique(array_map(static fn(Consumer\Record $record) => $record->partition, $records)));
    }

    /**
     * @return array<int, list<string>> the member ids that joined each generation
     */
    private static function joins(TestHandler $log): array
    {
        $joins = [];

        foreach ($log->getRecords() as $record) {
            $generation = $record->context['generationId'] ?? null;
            $member = $record->context['memberId'] ?? null;

            if ($record->message === 'joined group' && \is_int($generation) && \is_string($member)) {
                $joins[$generation][] = $member;
            }
        }

        return $joins;
    }
}
