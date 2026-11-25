<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\Future;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Protocol\Error\TopicError;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * The tests create topics without the wait of the kit. A new topic is used at
 * once, so a broker may not know it yet.
 */
final readonly class TopicLookupTest
{
    private const int TOPICS = 30;

    #[Test]
    public function newProducerFindsNewTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $futures = [];

            for ($i = 0; $i < self::TOPICS; ++$i) {
                $futures[] = async(static function () use ($kafka, $i): void {
                    $topic = self::createTopic($kafka);

                    $produced = $kafka->producer()->produceSync(new Producer\Record(value: "value-{$i}", topic: $topic));

                    Assert::same($produced->topic, $topic);
                    Assert::same($produced->partition, 0);
                    Assert::same($produced->offset, 0);
                });
            }

            Future\await($futures);
        });
    }

    #[Test]
    public function newDirectConsumerFindsNewTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $producer = $kafka->producer();
            $futures = [];

            for ($i = 0; $i < self::TOPICS; ++$i) {
                $futures[] = async(static function () use ($kafka, $producer, $i): void {
                    $topic = self::createTopic($kafka);

                    $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                        autoOffsetReset: AutoOffsetReset::Earliest,
                    ));

                    // The consumer starts first, so it looks up the topic before the producer does.
                    $consuming = async(static fn(): array => $kafka->consume($consumer, 1));

                    $producer->produceSync(new Producer\Record(value: "value-{$i}", topic: $topic));

                    /** @var list<Consumer\Record> $records */
                    $records = $consuming->await();

                    Assert::count($records, 1);
                    \assert(\count($records) === 1);
                    Assert::same($records[0]->topic, $topic);
                    Assert::same($records[0]->partition, 0);
                    Assert::same($records[0]->offset, 0);
                    Assert::same($records[0]->value, "value-{$i}");
                });
            }

            Future\await($futures);
        });
    }

    #[Test]
    public function newGroupConsumerFindsNewTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $producer = $kafka->producer();
            $futures = [];

            for ($i = 0; $i < self::TOPICS; ++$i) {
                $futures[] = async(static function () use ($kafka, $producer, $i): void {
                    $topic = self::createTopic($kafka);

                    $consumer = $kafka->groupConsumer($topic);

                    // The consumer starts first, so it looks up the topic before the producer does.
                    $consuming = async(static fn(): array => $kafka->consume($consumer, 1));

                    $producer->produceSync(new Producer\Record(value: "value-{$i}", topic: $topic));

                    /** @var list<Consumer\Record> $records */
                    $records = $consuming->await();

                    Assert::count($records, 1);
                    \assert(\count($records) === 1);
                    Assert::same($records[0]->topic, $topic);
                    Assert::same($records[0]->partition, 0);
                    Assert::same($records[0]->offset, 0);
                    Assert::same($records[0]->value, "value-{$i}");
                });
            }

            Future\await($futures);
        });
    }

    #[Test]
    public function produceToMissingTopicFailsAfterRetries(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topicName();
            $producer = $kafka->producer();

            $startedAt = hrtime(true);

            try {
                $producer->produceSync(new Producer\Record(value: 'value', topic: $topic));
                Assert::fail('Produce to a missing topic must fail.');
            } catch (UnknownTopic $e) {
                Assert::same($e->topic, $topic);
            }

            $elapsed = (hrtime(true) - $startedAt) / 1e9;

            // The lookup asks again with a backoff. The shortest backoff sum is 0.75 seconds.
            Assert::float($elapsed)->greaterThan(0.7)->lessThan(5.0);
        });
    }

    #[Test]
    public function produceToInvalidTopicFailsAtOnceWithTopicError(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $producer = $kafka->producer();

            $startedAt = hrtime(true);

            try {
                $producer->produceSync(new Producer\Record(value: 'value', topic: 'invalid topic name!'));
                Assert::fail('Produce to an invalid topic must fail.');
            } catch (ProtocolError $e) {
                $errors = $e->errors->ofCode(ErrorCode::InvalidTopicException);
                Assert::count($errors, 1);
                $error = $errors[0] ?? null;
                Assert::instanceOf($error, TopicError::class);
                Assert::same($error->topic, 'invalid topic name!');
            }

            $elapsed = (hrtime(true) - $startedAt) / 1e9;

            // A not retriable error is not asked again.
            Assert::float($elapsed)->lessThan(0.5);
        });
    }

    #[Test]
    public function producerSeesNewPartitionsAfterMetadataMaxAge(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopic($kafka);

            $producer = $kafka
                ->createClient(metadataMaxAge: TimeSpan::fromSeconds(1))
                ->createProducer();

            try {
                $first = $producer->produceSync(new Producer\Record(value: 'first', topic: $topic, partition: 0));
                Assert::same($first->partition, 0);
                Assert::same($first->offset, 0);

                $response = $kafka->client->request(new Generated\CreatePartitionsRequest(
                    topics: [
                        new Generated\CreatePartitionsRequest\CreatePartitionsTopic(
                            name: $topic,
                            count: 3,
                            assignments: null,
                        ),
                    ],
                    timeoutMs: 10_000,
                    validateOnly: false,
                ));
                Assert::same($response->results[0]->errorCode ?? null, ErrorCode::None->value);

                $produced = $kafka
                    ->poll(static function (Cancellation $_) use ($producer, $topic): ?Producer\Produced {
                        try {
                            return $producer->produceSync(new Producer\Record(value: 'new', topic: $topic, partition: 2));
                        } catch (UnknownPartition) {
                            return null;
                        }
                    })
                    ->expecting('a record in the new partition 2')
                    ->until(static fn(?Producer\Produced $produced) => $produced !== null);

                \assert($produced !== null);
                Assert::same($produced->topic, $topic);
                Assert::same($produced->partition, 2);
                Assert::same($produced->offset, 0);
            } finally {
                $producer->close();
            }
        });
    }

    #[Test]
    public function concurrentProducesOnNewProducerSucceed(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopic($kafka);
            $producer = $kafka->producer();

            $futures = [];
            for ($i = 0; $i < 20; ++$i) {
                $futures[] = async(static fn(): Producer\Produced => $producer->produceSync(new Producer\Record(value: "value-{$i}", topic: $topic)));
            }

            /** @var list<Producer\Produced> $results */
            $results = Future\await($futures);

            $offsets = [];
            foreach ($results as $produced) {
                Assert::same($produced->topic, $topic);
                Assert::same($produced->partition, 0);
                $offsets[] = $produced->offset;
            }

            sort($offsets);
            Assert::same($offsets, range(0, 19));
        });
    }

    /**
     * Creates a topic and does not wait until the brokers know it.
     *
     * @return non-empty-string
     */
    private static function createTopic(Kafka $kafka): string
    {
        $topic = $kafka->topicName();

        $response = $kafka->client->request(new Generated\CreateTopicsRequest(
            topics: [
                new Generated\CreateTopicsRequest\CreatableTopic(
                    name: $topic,
                    numPartitions: 1,
                    replicationFactor: 1,
                    assignments: [],
                    configs: [],
                ),
            ],
            timeoutMs: 10_000,
            validateOnly: false,
        ));

        Assert::same($response->topics[0]->errorCode ?? null, ErrorCode::None->value);

        return $topic;
    }
}
