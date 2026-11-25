<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Testo\Assert;
use Testo\Data\DataProvider;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Protocol\Error\PartitionError;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated\ApiKey;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\KafkaRefused;
use Thesis\Kafka\Testing\Setup;

/**
 * Produce v13 addresses a topic by its id (KIP-516).
 */
final readonly class ProduceByTopicIdTest
{
    #[Test]
    public function idempotentProducerSendsRecordsThatConsumerReads(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            Assert::int($kafka->negotiatedVersion(ApiKey::Produce) ?? -1)->greaterThanOrEqual(13);

            $topic = $kafka->topic();
            $producer = $kafka->producer(new Producer\Config(
                manualFlush: true,
                idempotent: true,
            ));

            $futures = [];
            for ($i = 0; $i < 100; ++$i) {
                $futures[] = $producer->produce(new Record(value: "record-{$i}", topic: $topic));
            }

            $producer->flush();

            /** @var list<Produced> $produced */
            $produced = Future\await($futures);
            ksort($produced);

            Assert::same(
                array_map(static fn(Produced $result) => [$result->topic, $result->partition, $result->offset], $produced),
                array_map(static fn(int $i) => [$topic, 0, $i], range(0, 99)),
            );

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            $records = $kafka->consume($consumer, count: 100);

            Assert::same(
                array_map(static fn(Consumer\Record $record) => [$record->offset, $record->value], $records),
                array_map(static fn(int $i) => [$i, "record-{$i}"], range(0, 99)),
            );
        });
    }

    #[Test]
    public function plainProducerSendsToEveryPartition(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            Assert::int($kafka->negotiatedVersion(ApiKey::Produce) ?? -1)->greaterThanOrEqual(13);

            $topic = $kafka->topic(partitions: 3);
            $producer = $kafka->producer();

            $produced = $producer->produceSync([
                new Record(value: 'p0-a', topic: $topic, partition: 0),
                new Record(value: 'p1-a', topic: $topic, partition: 1),
                new Record(value: 'p2-a', topic: $topic, partition: 2),
                new Record(value: 'p0-b', topic: $topic, partition: 0),
                new Record(value: 'p1-b', topic: $topic, partition: 1),
                new Record(value: 'p2-b', topic: $topic, partition: 2),
            ]);
            // The results come in the order of completion. Their keys are the order of the records.
            ksort($produced);

            Assert::same(
                array_map(static fn(Produced $result) => [$result->topic, $result->partition, $result->offset], $produced),
                [
                    [$topic, 0, 0],
                    [$topic, 1, 0],
                    [$topic, 2, 0],
                    [$topic, 0, 1],
                    [$topic, 1, 1],
                    [$topic, 2, 1],
                ],
            );
        });
    }

    #[Test]
    public function producerSendsToTopicCreatedAgain(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            Assert::int($kafka->negotiatedVersion(ApiKey::Produce) ?? -1)->greaterThanOrEqual(13);

            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $first = $producer->produceSync(new Record(value: 'before', topic: $topic));
            Assert::same($first->offset, 0);

            $kafka->deleteTopic($topic);

            // The broker can refuse the name for a short time after the deletion.
            $kafka->poll(static function () use ($kafka, $topic): bool {
                try {
                    $kafka->topic(name: $topic);

                    return true;
                } catch (KafkaRefused) {
                    return false;
                }
            })
                ->expecting("topic '{$topic}' created again")
                ->until(static fn(bool $created) => $created);

            $second = $producer->produceSync(new Record(value: 'after', topic: $topic));

            Assert::same($second->topic, $topic);
            Assert::same($second->partition, 0);
            Assert::same($second->offset, 0);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            $records = $kafka->consume($consumer, count: 1);
            \assert(\count($records) === 1);

            Assert::same($records[0]->value, 'after');
        });
    }

    #[Test]
    #[DataProvider([Kafka::class, 'versions'])]
    public function brokerErrorFailsRecordFuture(Setup $setup): void
    {
        Expect::exception(ProtocolError::class);

        $setup->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(configs: ['max.message.bytes' => '1024']);
            $producer = $kafka->producer();

            try {
                $producer->produceSync(new Record(value: str_repeat('x', 4_096), topic: $topic));
            } catch (ProtocolError $e) {
                $errors = $e->errors->errors;
                \assert(\count($errors) === 1 && $errors[0] instanceof PartitionError);

                Assert::same($errors[0]->code(), ErrorCode::MessageTooLarge);
                Assert::same($errors[0]->topic, $topic);
                Assert::same($errors[0]->partition, 0);

                throw $e;
            }
        });
    }
}
