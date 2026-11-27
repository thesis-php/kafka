<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Exception\ProduceTimedOut;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\ProduceLog;
use Thesis\Time\TimeSpan;

final readonly class ProduceTimeoutTest
{
    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function timedOutAttemptIsRetriedOnNewConnection(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromMilliseconds(500),
                    produceAttempts: 3,
                ),
                requestTimeout: TimeSpan::fromMilliseconds(500),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            $accepted = $proxy->accepted();

            // The open connections stay silent. The connection of the next attempt works.
            $proxy->stall();
            $produced = $producer->produceSync(new Record(value: 'second', topic: $topic));

            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 1]);
            Assert::same($log->attempts(), [1, 1, 2]);
            Assert::int($proxy->accepted())->greaterThan($accepted);

            $retries = $log->records('produce failed, retrying');
            Assert::same(\count($retries), 1);
            Assert::instanceOf($retries[0]->context['exception'] ?? null, ProduceTimedOut::class);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function lastTimedOutAttemptFailsRecord(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromMilliseconds(500),
                    produceAttempts: 2,
                ),
                requestTimeout: TimeSpan::fromMilliseconds(500),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // Every attempt gets no answer, also on a new connection.
            $proxy->silence();

            $failure = null;

            try {
                $producer->produceSync(new Record(value: 'second', topic: $topic));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, ProduceTimedOut::class);
            Assert::same([$failure->topic, $failure->partition], [$topic, 0]);
            Assert::same($log->attempts(), [1, 1, 2]);
        });
    }

    #[Test]
    #[Retry(maxAttempts: 5, markFlaky: true)]
    public function attemptWaitsForProduceAndRequestTimeout(): void
    {
        $log = new ProduceLog();

        Kafka::connect(Kafka::PROXIED_BROKER)->logger($log->logger())->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $topic = $kafka->topic();
            [$proxy, $producer] = $kafka->proxiedProducer(
                new Producer\Config(
                    produceTimeout: TimeSpan::fromSeconds(1),
                    produceAttempts: 3,
                ),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            Assert::same($producer->produceSync(new Record(value: 'first', topic: $topic))->offset, 0);

            // The answer takes longer than each of the two timeouts, but not longer than both.
            $proxy->delayAnswers(1.5);
            $produced = $producer->produceSync(new Record(value: 'second', topic: $topic));

            Assert::same([$produced->topic, $produced->partition, $produced->offset], [$topic, 0, 1]);
            Assert::same($log->attempts(), [1, 1]);
        });
    }
}
