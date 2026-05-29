<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Exception\ConsumerBusy;
use function Amp\async;

#[Covers(Consumer::class)]
#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class ConsumerTest extends TestCase
{
    #[Test]
    public function earliestConsumesMessagesProducedEarlier(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-consumer');
        $this->waitUntilTopicVisible($client, $topic);

        $producer = $client->createProducer(new Producer\Config());
        $produced = $producer->produceSync([
            new Producer\Record(value: 'old-0', topic: $topic, partition: 0),
            new Producer\Record(value: 'old-1', topic: $topic, partition: 0),
        ]);

        \assert(\count($produced) === 2);

        Assert::same($produced[0]->offset, 0);
        Assert::same($produced[1]->offset, 1);

        $consumer = $client->createConsumer($topic, new Consumer\Config(
            autoOffsetReset: AutoOffsetReset::Earliest,
        ));

        $records = $this->consumeUntilCount($consumer, 2);
        \assert(\count($records) === 2);

        Assert::count($records, 2);
        Assert::same($records[0]->value, 'old-0');
        Assert::same($records[1]->value, 'old-1');
        Assert::same($records[0]->offset, 0);
        Assert::same($records[1]->offset, 1);

        $consumer->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function latestConsumesOnlyMessagesProducedAfterStart(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-consumer');
        $this->waitUntilTopicVisible($client, $topic);

        $producer = $client->createProducer(new Producer\Config());
        $old = $producer->produceSync([
            new Producer\Record(value: 'old-0', topic: $topic, partition: 0),
            new Producer\Record(value: 'old-1', topic: $topic, partition: 0),
        ]);

        \assert(\count($old) === 2);

        Assert::same($old[0]->offset, 0);
        Assert::same($old[1]->offset, 1);

        $consumer = $client->createConsumer($topic, new Consumer\Config(
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

        $records = $this->consumeUntilCount($consumer, 2);
        \assert(\count($records) === 2);

        Assert::count($records, 2);
        Assert::same($records[0]->value, 'new-0');
        Assert::same($records[1]->value, 'new-1');
        Assert::same($records[0]->offset, 2);
        Assert::same($records[1]->offset, 3);

        $consumer->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function concurrentConsumeThrowsConsumerBusy(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-consumer');
        $this->waitUntilTopicVisible($client, $topic);

        $consumer = $client->createConsumer($topic, new Consumer\Config(
            autoOffsetReset: AutoOffsetReset::Latest,
        ));

        $first = async(static fn(): array => iterator_to_array($consumer->consume()));

        try {
            $consumer->consume(
                ConsumeMode::Poll,
                new TimeoutCancellation(0.1),
            );

            Assert::fail('Expected ConsumerBusy to be thrown.');
        } catch (ConsumerBusy) {
            $consumer->close();
            Assert::array($first->await())->hasCount(0);
        } finally {
            $this->deleteTopic($client, $topic);
            $client->close();
        }
    }

    /**
     * @param positive-int $expected
     * @return list<Consumer\Record>
     */
    private function consumeUntilCount(Consumer $consumer, int $expected): array
    {
        $records = [];

        for ($attempt = 0; $attempt < 10 && \count($records) < $expected; ++$attempt) {
            $batches = $consumer->consume(
                ConsumeMode::Poll,
                new TimeoutCancellation(0.5),
            );

            foreach ($batches as $batch) {
                foreach ($batch as $record) {
                    $records[] = $record;
                }
            }
        }

        return $records;
    }
}
