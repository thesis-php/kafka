<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Exception\ConsumerBusy;
use Thesis\Kafka\Exception\InvalidConfiguration;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\PeerFault;
use Thesis\Time\TimeSpan;
use function Amp\async;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class DirectConsumerTest
{
    #[Test]
    public function earliestConsumesMessagesProducedEarlier(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $produced = $producer->produceSync([
                new Producer\Record(value: 'old-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'old-1', topic: $topic, partition: 0),
            ]);

            \assert(\count($produced) === 2);

            Assert::same($produced[0]->offset, 0);
            Assert::same($produced[1]->offset, 1);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            $records = $kafka->consume($consumer, count: 2);
            \assert(\count($records) === 2);

            Assert::count($records, 2);
            Assert::same($records[0]->value, 'old-0');
            Assert::same($records[1]->value, 'old-1');
            Assert::same($records[0]->offset, 0);
            Assert::same($records[1]->offset, 1);
        });
    }

    #[Test]
    public function latestConsumesOnlyMessagesProducedAfterStart(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $old = $producer->produceSync([
                new Producer\Record(value: 'old-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'old-1', topic: $topic, partition: 0),
            ]);

            \assert(\count($old) === 2);

            Assert::same($old[0]->offset, 0);
            Assert::same($old[1]->offset, 1);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Latest,
            ));

            $firstPoll = iterator_to_array(
                $consumer->consume(
                    ConsumeMode::Poll,
                    new TimeoutCancellation(0.2),
                ),
                false,
            );

            Assert::same($firstPoll, []);

            $new = $producer->produceSync([
                new Producer\Record(value: 'new-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'new-1', topic: $topic, partition: 0),
            ]);

            \assert(\count($new) === 2);

            Assert::same($new[0]->offset, 2);
            Assert::same($new[1]->offset, 3);

            $records = $kafka->consume($consumer, count: 2);
            \assert(\count($records) === 2);

            Assert::count($records, 2);
            Assert::same($records[0]->value, 'new-0');
            Assert::same($records[1]->value, 'new-1');
            Assert::same($records[0]->offset, 2);
            Assert::same($records[1]->offset, 3);
        });
    }

    #[Test]
    public function concurrentConsumeThrowsConsumerBusy(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Latest,
            ));

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
    public function callbackConsumeProcessesRecords(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $produced = $producer->produceSync([
                new Producer\Record(value: 'old-0', topic: $topic, partition: 0),
                new Producer\Record(value: 'old-1', topic: $topic, partition: 0),
            ]);

            \assert(\count($produced) === 2);

            $seen = [];

            $subscription = $kafka->subscribe(
                $topic,
                static function (array $records, ConsumerSession $session) use (&$seen): void {
                    foreach ($records as $record) {
                        $seen[] = $record;
                    }

                    if (\count($seen) >= 2) {
                        $session->close();
                    }
                },
            );

            $subscription->join();

            \assert(\count($seen) === 2);

            Assert::count($seen, 2);
            Assert::same($seen[0]->value, 'old-0');
            Assert::same($seen[1]->value, 'old-1');
            Assert::same($seen[0]->offset, 0);
            Assert::same($seen[1]->offset, 1);
        });
    }

    #[Test]
    public function callbackConsumeJoinRethrowsHandlerFailure(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $producer->produceSync(new Producer\Record(
                value: 'boom',
                topic: $topic,
                partition: 0,
            ));

            $subscription = $kafka->subscribe(
                $topic,
                static function (): void {
                    throw new \RuntimeException('handler boom');
                },
                new Consumer\Config(
                    maxWait: TimeSpan::fromMilliseconds(1),
                    maxBytes: 20,
                ),
            );

            try {
                $subscription->join();

                Assert::fail('Expected join() to rethrow the handler failure.');
            } catch (\RuntimeException $e) {
                Assert::same($e->getMessage(), 'handler boom');
            }
        });
    }

    #[Test]
    public function noneWithoutGroupIsRejected(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            // Every request to this peer fails. The config is rejected before a request.
            $peer = $kafka->brokenPeer(PeerFault::Close);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);
            $config = new Consumer\Config(autoOffsetReset: AutoOffsetReset::None);

            $failures = [];

            try {
                $client->createConsumer('any-topic', $config);
            } catch (InvalidConfiguration $e) {
                $failures[] = $e;
            }

            try {
                $client->consume('any-topic', static fn() => null, $config);
            } catch (InvalidConfiguration $e) {
                $failures[] = $e;
            }

            Assert::count($failures, 2);
            Assert::same($peer->accepted(), 0);

            // With a group the consumer uses the committed offset, so None is fine.
            $topic = $kafka->topic();
            $kafka->groupConsumer($topic, autoOffsetReset: AutoOffsetReset::None);
            $kafka->subscribe($topic, static fn() => null, $kafka->groupConfig(autoOffsetReset: AutoOffsetReset::None));
        });
    }
}
