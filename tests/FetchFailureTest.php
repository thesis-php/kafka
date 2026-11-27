<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Compressor\Codec;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Exception\UnsupportedCodec;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Time\TimeSpan;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class FetchFailureTest
{
    #[Test]
    public function directConsumerThrowsUnknownCodec(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::compressedTopic($kafka);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                decompressors: new Compressor\Registry(),
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            self::assertUnknownCodec($kafka, $consumer);
        });
    }

    #[Test]
    public function groupConsumerThrowsUnknownCodecAndLeaves(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::compressedTopic($kafka);
            $groupId = $kafka->groupId();

            $consumer = $kafka->groupConsumer($topic, groupId: $groupId, decompressors: new Compressor\Registry());

            self::assertUnknownCodec($kafka, $consumer);

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
    public function subscriptionJoinThrowsUnknownCodec(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $topic = self::compressedTopic($kafka);

            $subscription = $kafka->subscribe(
                $topic,
                static fn() => Assert::fail('The handler must not get records.'),
                new Consumer\Config(decompressors: new Compressor\Registry(), autoOffsetReset: AutoOffsetReset::Earliest),
            );

            $failure = null;

            try {
                $subscription->join();
            } catch (UnsupportedCodec $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, UnsupportedCodec::class);
            Assert::same($failure->codec, Codec::Gzip);
        });
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

    private static function assertUnknownCodec(Kafka $kafka, Consumer $consumer): void
    {
        $failure = null;

        $errors = $kafka->loopErrorsDuring(static function () use ($consumer, &$failure): void {
            try {
                foreach ($consumer->consume(cancellation: new TimeoutCancellation(10)) as $_) {
                    Assert::fail('The consumer must not read records.');
                }
            } catch (UnsupportedCodec $e) {
                $failure = $e;
            }
        });

        Assert::instanceOf($failure, UnsupportedCodec::class);
        Assert::same($failure->codec, Codec::Gzip);
        Assert::same($errors, []);
    }
}
