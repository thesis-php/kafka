<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Kafka\Config\ProducerConfig;
use Thesis\Kafka\Internal\Producer;
use Thesis\Kafka\Partitioner\RoundRobinPartitioner;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;

#[Covers(Client::class)]
#[Covers(Producer::class)]
final readonly class ProducerTest extends TestCase
{
    #[Test]
    public function lingerDelaysFutureResolution(): void
    {
        $admin = $this->createClient();
        $client = $this->createClient(producer: new ProducerConfig(
            linger: TimeSpan::fromMilliseconds(300),
        ));

        $topic = $this->createTopic($admin, partitions: 1);
        $this->waitUntilTopicVisible($admin, $topic, partitions: 1);
        $this->waitUntilTopicVisible($client, $topic, partitions: 1);

        $startedAt = $this->nowMs();
        $future = $client->produce(new ProduceRecord(
            value: 'linger-test',
            topic: $topic,
        ));

        $result = $future->await();
        $elapsed = $this->nowMs() - $startedAt;

        Assert::int($elapsed)->greaterThanOrEqual(200)->lessThan(1_000);
        Assert::same($result->topic, $topic);
        Assert::same($result->partition, 0);
        Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);

        $this->deleteTopic($admin, $topic);
        $admin->close();
        $client->close();
    }

    #[Test]
    public function fullBatchBypassesLingerAndResolvesImmediately(): void
    {
        $admin = $this->createClient();
        $client = $this->createClient(producer: new ProducerConfig(
            linger: TimeSpan::fromMilliseconds(3_000),
            batchSize: 256,
            partitioner: new RoundRobinPartitioner(),
        ));

        $topic = $this->createTopic($admin, partitions: 3);
        $this->waitUntilTopicVisible($admin, $topic, partitions: 3);
        $this->waitUntilTopicVisible($client, $topic, partitions: 3);

        $startedAt = $this->nowMs();

        $results = $client->produce([
            new ProduceRecord(
                value: str_repeat('a', 180),
                topic: $topic,
                partition: 2,
            ),
            new ProduceRecord(
                value: str_repeat('b', 180),
                topic: $topic,
                partition: 2,
            ),
        ]);

        /** @var list<ProduceResult> $results */
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

        $this->deleteTopic($admin, $topic);
        $admin->close();
        $client->close();
    }

    #[Test]
    public function closeDrainsBufferedRecordsWithoutWaitingForLinger(): void
    {
        $admin = $this->createClient();
        $client = $this->createClient(producer: new ProducerConfig(
            linger: TimeSpan::fromMilliseconds(3_000),
        ));

        $topic = $this->createTopic($admin, partitions: 1);
        $this->waitUntilTopicVisible($admin, $topic, partitions: 1);
        $this->waitUntilTopicVisible($client, $topic, partitions: 1);

        $future = $client->produce(new ProduceRecord(
            value: 'close-drain-test',
            topic: $topic,
        ));

        $startedAt = $this->nowMs();
        $client->close();
        $result = $future->await();
        $elapsed = $this->nowMs() - $startedAt;

        Assert::int($elapsed)->lessThan(100);
        Assert::same($result->topic, $topic);
        Assert::same($result->partition, 0);
        Assert::int($result->offset ?? -1)->greaterThanOrEqual(0);

        $this->deleteTopic($admin, $topic);
        $admin->close();
    }

    #[Test]
    public function manualFlushKeepsProduceFuturesPendingUntilExplicitFlush(): void
    {
        $admin = $this->createClient();
        $client = $this->createClient(producer: new ProducerConfig(
            manualFlush: true,
        ));

        $topic = $this->createTopic($admin, partitions: 3);
        $this->waitUntilTopicVisible($admin, $topic, partitions: 3);
        $this->waitUntilTopicVisible($client, $topic, partitions: 3);

        $futures = $client->produce([
            new ProduceRecord(
                value: 'manual-flush-0',
                topic: $topic,
                partition: 2,
            ),
            new ProduceRecord(
                value: 'manual-flush-1',
                topic: $topic,
                partition: 2,
            ),
        ]);

        \assert(\count($futures) === 2);

        usleep(200_000);

        Assert::false($futures[0]->isComplete());
        Assert::false($futures[1]->isComplete());

        $client->flush();

        Assert::true($futures[0]->isComplete());
        Assert::true($futures[1]->isComplete());

        /** @var list<ProduceResult> $results */
        $results = array_values(Future\await($futures));

        \assert(\count($results) === 2);

        Assert::same($results[0]->topic, $topic);
        Assert::same($results[1]->topic, $topic);
        Assert::same($results[0]->partition, 2);
        Assert::same($results[1]->partition, 2);
        Assert::int($results[0]->offset ?? -1)->greaterThanOrEqual(0);
        Assert::same($results[1]->offset, ($results[0]->offset ?? 0) + 1);

        $this->deleteTopic($admin, $topic);
        $admin->close();
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
