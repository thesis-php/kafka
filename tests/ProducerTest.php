<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Producer\Config;
use Thesis\Kafka\Producer\Internal\AsyncProducer;
use Thesis\Kafka\Producer\Partitioner\RoundRobinPartitioner;
use Thesis\Kafka\Producer\Produced;
use Thesis\Kafka\Producer\Record;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;

#[Covers(Client::class)]
#[Covers(AsyncProducer::class)]
#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class ProducerTest extends TestCase
{
    #[Test]
    public function lingerDelaysFutureResolution(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopic($client, partitions: 1);
        $this->waitUntilTopicVisible($client, $topic, partitions: 1);

        $startedAt = $this->nowMs();
        $producer = $client->createProducer(new Config(
            linger: TimeSpan::fromMilliseconds(300),
        ));
        $future = $producer->produce(new Record(
            value: 'linger-test',
            topic: $topic,
        ));

        $result = $future->await();
        $elapsed = $this->nowMs() - $startedAt;

        Assert::int($elapsed)->greaterThanOrEqual(200)->lessThan(1_000);
        Assert::same($result->topic, $topic);
        Assert::same($result->partition, 0);
        Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);

        $this->deleteTopic($client, $topic);
        $producer->close();
        $client->close();
    }

    #[Test]
    public function fullBatchBypassesLingerAndResolvesImmediately(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopic($client, partitions: 3);
        $this->waitUntilTopicVisible($client, $topic, partitions: 3);

        $startedAt = $this->nowMs();

        $producer = $client->createProducer(new Config(
            linger: TimeSpan::fromMilliseconds(3_000),
            batchSize: 256,
            partitioner: new RoundRobinPartitioner(),
        ));

        $results = $producer->produce([
            new Record(
                value: str_repeat('a', 180),
                topic: $topic,
                partition: 2,
            ),
            new Record(
                value: str_repeat('b', 180),
                topic: $topic,
                partition: 2,
            ),
        ]);

        /** @var list<Produced> $results */
        $results = Future\await($results);
        $elapsed = $this->nowMs() - $startedAt;

        Assert::int($elapsed)->lessThan(1_000);
        \assert(\count($results) === 2);
        Assert::same($results[0]->topic, $topic);
        Assert::same($results[0]->partition, 2);
        Assert::int($results[0]->offset ?? -1)->greaterThanOrEqual(0);
        Assert::same($results[1]->topic, $topic);
        Assert::same($results[1]->partition, 2);
        Assert::same($results[1]->offset, ($results[0]->offset ?? 0) + 1);

        $this->deleteTopic($client, $topic);
        $producer->close();
        $client->close();
    }

    #[Test]
    public function closeDrainsBufferedRecordsWithoutWaitingForLinger(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopic($client, partitions: 1);
        $this->waitUntilTopicVisible($client, $topic, partitions: 1);

        $producer = $client->createProducer(new Config(
            linger: TimeSpan::fromMilliseconds(3_000),
        ));

        $future = $producer->produce(new Record(
            value: 'close-drain-test',
            topic: $topic,
        ));

        $startedAt = $this->nowMs();
        $producer->close();
        $result = $future->await();
        $elapsed = $this->nowMs() - $startedAt;

        Assert::int($elapsed)->lessThan(100);
        Assert::same($result->topic, $topic);
        Assert::same($result->partition, 0);
        Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);

        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function manualFlushKeepsProduceFuturesPendingUntilExplicitFlush(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopic($client, partitions: 3);
        $this->waitUntilTopicVisible($client, $topic, partitions: 3);

        $producer = $client->createProducer(new Config(
            manualFlush: true,
        ));

        $futures = $producer->produce([
            new Record(
                value: 'manual-flush-0',
                topic: $topic,
                partition: 2,
            ),
            new Record(
                value: 'manual-flush-1',
                topic: $topic,
                partition: 2,
            ),
        ]);

        \assert(\count($futures) === 2);

        usleep(200_000);

        Assert::false($futures[0]->isComplete());
        Assert::false($futures[1]->isComplete());

        $producer->flush();

        Assert::true($futures[0]->isComplete());
        Assert::true($futures[1]->isComplete());

        /** @var list<Produced> $results */
        $results = array_values(Future\await($futures));

        \assert(\count($results) === 2);

        Assert::same($results[0]->topic, $topic);
        Assert::same($results[1]->topic, $topic);
        Assert::same($results[0]->partition, 2);
        Assert::same($results[1]->partition, 2);
        Assert::int($results[0]->offset ?? -1)->greaterThanOrEqual(0);
        Assert::same($results[1]->offset, ($results[0]->offset ?? 0) + 1);

        $this->deleteTopic($client, $topic);
        $producer->close();
        $client->close();
    }

    /**
     * @return non-empty-string
     */
    private function createTopic(Client $client, int $partitions): string
    {
        $topicName = $this->topicName('thesis-producer');

        $response = $client->request(new Generated\CreateTopicsRequest(
            topics: [
                new Generated\CreateTopicsRequest\CreatableTopic(
                    name: $topicName,
                    numPartitions: $partitions,
                    replicationFactor: 1,
                    assignments: [],
                    configs: [],
                ),
            ],
            timeoutMs: 10_000,
            validateOnly: false,
        ));

        Assert::array($response->topics)->hasCount(1);
        Assert::same($response->topics[0]->name ?? null, $topicName);
        Assert::same($response->topics[0]->errorCode ?? null, 0);

        return $topicName;
    }

    private function deleteTopic(Client $client, string $topicName): void
    {
        $response = $client->request(new Generated\DeleteTopicsRequest(
            topics: [
                new Generated\DeleteTopicsRequest\DeleteTopicState(
                    name: $topicName,
                    topicId: Uuid::fromString(Uuid::NIL),
                ),
            ],
            topicNames: [$topicName],
            timeoutMs: 10_000,
        ));

        Assert::array($response->responses)->hasCount(1);
        Assert::same($response->responses[0]->name ?? null, $topicName);
        Assert::same($response->responses[0]->errorCode ?? null, 0);
    }

    private function waitUntilTopicVisible(Client $client, string $topicName, int $partitions): void
    {
        for ($attempt = 0; $attempt < 10; ++$attempt) {
            $response = $client->request(new Generated\MetadataRequest(
                topics: [
                    new Generated\MetadataRequest\MetadataRequestTopic(
                        topicId: Uuid::fromString(Uuid::NIL),
                        name: $topicName,
                    ),
                ],
                allowAutoTopicCreation: false,
                includeClusterAuthorizedOperations: false,
                includeTopicAuthorizedOperations: false,
            ));

            if (
                \count($response->topics) === 1
                && $response->topics[0]->name === $topicName
                && $response->topics[0]->errorCode === 0
                && \count($response->topics[0]->partitions) === $partitions
                && array_filter(
                    $response->topics[0]->partitions,
                    static fn(Generated\MetadataResponse\MetadataResponseTopic\MetadataResponsePartition $partition) => $partition->errorCode === 0 && $partition->leaderId >= 0,
                ) === $response->topics[0]->partitions
            ) {
                return;
            }

            usleep(200_000);
        }

        Assert::fail("Topic '{$topicName}' did not become visible in metadata.");
    }

    /**
     * @return non-negative-int
     */
    private function nowMs(): int
    {
        /** @var non-negative-int */
        return intdiv((int) hrtime(true), 1_000_000);
    }
}
