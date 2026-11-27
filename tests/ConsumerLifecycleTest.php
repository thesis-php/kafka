<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Exception\ConsumerBusy;
use Thesis\Kafka\Exception\ConsumerClosed;
use Thesis\Kafka\Exception\RequestTimedOut;
use Thesis\Kafka\Exception\UnsupportedCodec;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\PeerFault;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class ConsumerLifecycleTest
{
    #[Test]
    public function directDroppedIterableFreesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 1);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest));
            $consumer->consume();

            Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);
        });
    }

    #[Test]
    public function groupDroppedIterableFreesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 1);

            $consumer = $kafka->groupConsumer($topic);
            $consumer->consume();

            Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);
        });
    }

    #[Test]
    public function groupConcurrentConsumeThrowsConsumerBusy(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $consumer = $kafka->groupConsumer($kafka->topic());

            $futures = [];
            $futures[] = async($consumer->consume(...));
            $futures[] = async($consumer->consume(...));

            try {
                Future\await($futures);

                Assert::fail('Expected ConsumerBusy to be thrown.');
            } catch (ConsumerBusy $e) {
                Assert::same(
                    $e->getMessage(),
                    'consume() is already running on this consumer; only one consume() loop may be active at a time.',
                );
            }
        });
    }

    #[Test]
    public function directCancellationKeepsBufferedBatches(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 10);

            self::assertCancellationKeepsBatches(
                $kafka,
                $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest)),
            );
        });
    }

    #[Test]
    public function groupCancellationKeepsBufferedBatches(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 10);

            self::assertCancellationKeepsBatches($kafka, $kafka->groupConsumer($topic));
        });
    }

    #[Test]
    public function directYieldsNoBatchAfterClose(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 10);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest));

            $seen = [];

            foreach ($consumer->consume(cancellation: new TimeoutCancellation(10)) as $batch) {
                $seen[] = self::values($batch);
                $consumer->close();
            }

            Assert::same($seen, [['p0-0']]);
        });
    }

    #[Test]
    public function directPollReturnsBuffer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 10);

            self::assertPollReturnsBuffer(
                $kafka,
                $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest)),
            );
        });
    }

    #[Test]
    public function groupPollReturnsBuffer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 10);

            self::assertPollReturnsBuffer($kafka, $kafka->groupConsumer($topic));
        });
    }

    #[Test]
    public function directPollWithNothingIsEmpty(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $consumer = $kafka->directConsumer($kafka->topic(), new Consumer\Config(
                pollTimeout: TimeSpan::fromSeconds(0.5),
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            // Without a cancellation the poll timeout of the config limits the wait.
            [$batches, $elapsed] = self::measure(static fn() => iterator_to_array($consumer->consume(ConsumeMode::Poll), false));

            Assert::same($batches, []);
            Assert::true($elapsed >= 0.4, \sprintf('Poll ended after %.2f s.', $elapsed));
        });
    }

    #[Test]
    public function groupPollWithNothingIsEmpty(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $consumer = $kafka->groupConsumer($kafka->topic());

            [$batches, $elapsed] = self::measure(static fn() => iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(0.5)), false));

            Assert::same($batches, []);
            Assert::true($elapsed >= 0.4, \sprintf('Poll ended after %.2f s.', $elapsed));
        });
    }

    #[Test]
    public function directUsesPrefetchBufferSize(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 20);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
                prefetchBufferSize: 1,
            ));

            $first = iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(10)), false);

            // Gives the fetch time to fill the buffer again.
            delay(0.5);

            $second = iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(10)), false);

            // The buffer holds one batch. The fetch waits with one more batch.
            Assert::count($second, 2);

            $values = array_merge(...array_map(self::values(...), [...$first, ...$second]));
            Assert::same($values, array_map(static fn(int $i) => "p0-{$i}", range(0, \count($values) - 1)));
        });
    }

    #[Test]
    public function directFatalErrorClosesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $consumer = $kafka->directConsumer(self::compressedTopic($kafka), new Consumer\Config(
                decompressors: new Compressor\Registry(),
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            self::assertFatalErrorCloses($consumer);
        });
    }

    #[Test]
    public function groupFatalErrorClosesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $consumer = $kafka->groupConsumer(self::compressedTopic($kafka), decompressors: new Compressor\Registry());

            self::assertFatalErrorCloses($consumer);
        });
    }

    #[Test]
    public function groupFatalErrorReleasesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::compressedTopic($kafka);
            $groupId = $kafka->groupId();

            $consumer = $kafka->groupConsumer(
                $topic,
                groupId: $groupId,
                autocommit: true,
                autocommitInterval: TimeSpan::fromSeconds(0.2),
                decompressors: new Compressor\Registry(),
            );

            $work = $kafka->loopWorkAfter(static function () use ($consumer): void {
                try {
                    foreach ($consumer->consume(cancellation: new TimeoutCancellation(10)) as $_) {
                        Assert::fail('The consumer must not read records.');
                    }
                } catch (UnsupportedCodec) {
                    // The fatal error of this scenario.
                }
            });

            Assert::same($work, []);

            // The session timeout is 6 s. A member that is gone sooner has left the group.
            $kafka
                ->poll(static fn(Cancellation $_) => $kafka->groupMembers($groupId))
                ->expecting('a group without members')
                ->within(TimeSpan::fromSeconds(3))
                ->describedBy(static fn(array $members) => implode(', ', $members))
                ->until(static fn(array $members) => $members === []);
        });
    }

    #[Test]
    public function groupTimeLimitThrowsRequestTimedOut(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->timeout(TimeSpan::fromSeconds(30))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 1);

            [$proxy, $consumer] = $kafka->proxiedGroupConsumer(
                $topic,
                requestTimeout: TimeSpan::fromSeconds(1),
                heartbeatInterval: TimeSpan::fromSeconds(0.3),
            );

            Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);

            $proxy->silence();

            $failure = null;

            try {
                foreach ($consumer->consume(cancellation: new TimeoutCancellation(20)) as $_) {
                    Assert::fail('The consumer must not read records.');
                }
            } catch (RequestTimedOut $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, RequestTimedOut::class);
            Assert::instanceOf($failure->getPrevious(), CancelledException::class);
            Assert::same(self::closedError($consumer), ConsumerClosed::class);
        });
    }

    #[Test]
    public function closeBeforeConsumeClosesConsumer(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            // Every connection to this peer fails. The consumers must not connect.
            $peer = $kafka->brokenPeer(PeerFault::Close);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);

            $direct = $kafka->directConsumer('any-topic', new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest), $client);
            $group = $kafka->groupConsumer('any-topic', client: $client);

            $direct->close();
            $group->close();

            Assert::same(self::closedError($direct), ConsumerClosed::class);
            Assert::same(self::closedError($group), ConsumerClosed::class);
            Assert::same($peer->accepted(), 0);
        });
    }

    #[Test]
    public function directFailedSetupRunsAgain(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 1);

            [$proxy, $consumer] = $kafka->proxiedConsumer(
                $topic,
                new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            $proxy->disconnect();

            $failure = null;

            try {
                foreach ($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(10)) as $_) {
                    Assert::fail('The consumer must not read records.');
                }
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::notNull($failure);
            Assert::true(!$failure instanceof ConsumerClosed, 'The consumer must not be closed.');

            $proxy->reconnect();

            Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);
        });
    }

    #[Test]
    public function directCloseDuringSetupWins(): void
    {
        Kafka::connect(Kafka::PROXIED_BROKER)->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            [$proxy, $consumer] = $kafka->proxiedConsumer(
                $topic,
                new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest),
                requestTimeout: TimeSpan::fromSeconds(10),
            );

            $proxy->silence();

            $read = async(static fn() => iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(20)), false));

            // Gives the setup time to send its first request.
            delay(0.5);

            $consumer->close();

            Assert::same($read->await(new TimeoutCancellation(5)), []);
            Assert::same(self::closedError($consumer), ConsumerClosed::class);
        });
    }

    #[Test]
    public function subscriptionClosedFromHandler(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(15))->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            self::produceBatches($kafka->producer(), $topic, count: 5);

            /** @var list<list<?string>> $seen */
            $seen = [];
            $closed = false;
            /** @var ?ConsumerSubscription $subscription */
            $subscription = null;

            $subscription = $kafka->subscribe(
                $topic,
                static function (array $records) use (&$subscription, &$seen, &$closed): void {
                    $seen[] = self::values($records);

                    \assert($subscription instanceof ConsumerSubscription);
                    $subscription->close();
                    $closed = true;
                },
                new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest),
            );

            $subscription->join();

            Assert::true($closed, 'close() from the handler must return.');
            Assert::same($seen, [['p0-0']]);
        });
    }

    #[Test]
    public function subscriptionRunsWithoutHandle(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            /** @var list<?string> $seen */
            $seen = [];

            $kafka->client->consume(
                $topic,
                static function (array $records, ConsumerSession $session) use (&$seen): void {
                    $seen = [...$seen, ...self::values($records)];
                    $session->close();
                },
                new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest),
            );

            // The handle of the subscription is gone already. The record comes later.
            delay(0.5);
            self::produceBatches($kafka->producer(), $topic, count: 1);

            $values = $kafka
                ->poll(static function (Cancellation $_) use (&$seen): array {
                    /** @var list<?string> $seen */
                    return $seen;
                })
                ->expecting('the record in the handler')
                ->until(static fn(array $values) => $values !== []);

            Assert::same($values, ['p0-0']);
        });
    }

    private static function assertCancellationKeepsBatches(Kafka $kafka, Consumer $consumer): void
    {
        Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);

        // Gives the fetch time to fill the buffer. So batches are ready when the cancellation fires.
        delay(0.5);

        $cancellation = new DeferredCancellation();
        $seen = [];

        foreach ($consumer->consume(ConsumeMode::Stream, $cancellation->getCancellation()) as $batch) {
            $seen[] = self::values($batch);
            $cancellation->cancel();
        }

        Assert::same($seen, [['p0-1']]);

        $rest = self::values($kafka->consume($consumer, count: 8));
        Assert::same($rest, array_map(static fn(int $i) => "p0-{$i}", range(2, 9)));
    }

    private static function assertPollReturnsBuffer(Kafka $kafka, Consumer $consumer): void
    {
        Assert::same(self::values($kafka->consume($consumer, count: 1)), ['p0-0']);

        // Gives the fetch time to fill the buffer.
        delay(0.5);

        [$batches, $elapsed] = self::measure(static fn() => iterator_to_array($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(10)), false));

        Assert::true(\count($batches) > 1, \sprintf('Poll gave %d batches.', \count($batches)));
        Assert::true($elapsed < 2.0, \sprintf('Poll took %.2f s.', $elapsed));

        $values = array_merge(...array_map(self::values(...), $batches));
        Assert::same($values, array_map(static fn(int $i) => "p0-{$i}", range(1, \count($values))));
    }

    private static function assertFatalErrorCloses(Consumer $consumer): void
    {
        $failure = null;

        try {
            foreach ($consumer->consume(cancellation: new TimeoutCancellation(10)) as $_) {
                Assert::fail('The consumer must not read records.');
            }
        } catch (UnsupportedCodec $e) {
            $failure = $e;
        }

        Assert::instanceOf($failure, UnsupportedCodec::class);
        Assert::same(self::closedError($consumer), ConsumerClosed::class);

        $consumer->close();
    }

    /**
     * Returns the class of the exception of the next consume(), null when it ends without one.
     *
     * @return ?class-string<\Throwable>
     */
    private static function closedError(Consumer $consumer): ?string
    {
        try {
            foreach ($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(1)) as $_) {
                Assert::fail('A closed consumer must not read records.');
            }
        } catch (\Throwable $e) {
            return $e::class;
        }

        return null;
    }

    /**
     * @return non-empty-string a topic with one record compressed with gzip
     */
    private static function compressedTopic(Kafka $kafka): string
    {
        $topic = $kafka->topic();

        $kafka
            ->producer(new Producer\Config(compressor: new Compressor\Gzip()))
            ->produceSync(new Producer\Record(value: 'compressed', topic: $topic, partition: 0));

        return $topic;
    }

    /**
     * One produce call makes one record batch: p0-0, p0-1, ...
     *
     * @param non-empty-string $topic
     * @param positive-int $count
     */
    private static function produceBatches(Producer $producer, string $topic, int $count): void
    {
        for ($i = 0; $i < $count; ++$i) {
            $producer->produceSync(new Producer\Record(value: "p0-{$i}", topic: $topic, partition: 0));
        }
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
     * @template T
     * @param \Closure(): T $step
     * @return array{T, float}
     */
    private static function measure(\Closure $step): array
    {
        $start = hrtime(true);
        $result = $step();

        return [$result, (hrtime(true) - $start) / 1e9];
    }
}
