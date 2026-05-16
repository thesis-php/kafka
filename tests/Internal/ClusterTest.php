<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Internal\Cluster\Metadata\TopicKind;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TestCase;

final readonly class ClusterTest extends TestCase
{
    #[Test]
    public function metadataWithoutTopicsReturnsOnlyBrokers(): void
    {
        $cluster = $this->createCluster();

        $metadata = $cluster->metadata();

        Assert::array($metadata->brokers)->hasCount(3);
        Assert::array($metadata->topics)->hasCount(0);
        Assert::int($metadata->controllerId)->greaterThanOrEqual(-1);

        $cluster->close();
    }

    #[Test]
    public function metadataWithPreloadTopicsLoadsThem(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client);
        $this->waitUntilTopicVisible($client, $topic);

        $cluster = $this->createCluster(preloadTopics: [$topic]);
        $metadata = $this->waitUntilClusterMetadataContainsTopics(
            $cluster,
            [],
            [$topic],
        );

        Assert::array($metadata->topics)->hasKeys($topic);
        Assert::same($metadata->topics[$topic]->name ?? null, $topic);
        Assert::same($metadata->topics[$topic]->kind ?? null, TopicKind::User);

        $this->deleteTopic($client, $topic);

        $client->close();
    }

    #[Test]
    public function refreshMetadataMergesRequestedTopicsWithPreloadTopics(): void
    {
        $client = $this->createClient();
        $preloadedTopic = $this->createTopic($client, 'thesis-preload');
        $requestedTopic = $this->createTopic($client, 'thesis-request');
        $this->waitUntilTopicVisible($client, $preloadedTopic);
        $this->waitUntilTopicVisible($client, $requestedTopic);

        $cluster = $this->createCluster(preloadTopics: [$preloadedTopic]);
        $metadata = $this->waitUntilClusterMetadataContainsTopics(
            $cluster,
            [$requestedTopic],
            [$preloadedTopic, $requestedTopic],
        );

        Assert::array($metadata->topics)->hasKeys($preloadedTopic, $requestedTopic);
        Assert::same($metadata->topics[$preloadedTopic]->name ?? null, $preloadedTopic);
        Assert::same($metadata->topics[$requestedTopic]->name ?? null, $requestedTopic);

        $this->deleteTopic($client, $preloadedTopic);
        $this->deleteTopic($client, $requestedTopic);

        $client->close();
    }

    #[Test]
    public function refreshMetadataDropsDeletedTopics(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client);
        $this->waitUntilTopicVisible($client, $topic);

        $cluster = $this->createCluster(preloadTopics: [$topic]);
        $metadata = $this->waitUntilClusterMetadataContainsTopics(
            $cluster,
            [],
            [$topic],
        );

        Assert::array($metadata->topics)->hasKeys($topic);

        $this->deleteTopic($client, $topic);
        $this->waitUntilTopicDeleted($client, $topic);
        $metadata = $cluster->refreshMetadata([$topic]);

        Assert::array($metadata->topics)->hasCount(0);

        $client->close();
    }

    /**
     * @param list<non-empty-string> $preloadTopics
     */
    private function createCluster(array $preloadTopics = []): Cluster
    {
        return new Cluster(new Config(
            seeds: self::SEEDS,
            preloadTopics: $preloadTopics,
        ));
    }

    /**
     * @return non-empty-string
     */
    private function createTopic(Client $client, string $prefix = 'thesis-it'): string
    {
        $topicName = $this->topicName($prefix);

        $response = $client->request(new Generated\CreateTopicsRequest(
            topics: [
                new Generated\CreateTopicsRequest\CreatableTopic(
                    name: $topicName,
                    numPartitions: 1,
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

    private function waitUntilTopicVisible(Client $client, string $topicName): void
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

            if (\count($response->topics) === 1 && $response->topics[0]->name === $topicName && $response->topics[0]->errorCode === 0) {
                return;
            }

            usleep(200_000);
        }

        Assert::fail("Topic '{$topicName}' did not become visible in metadata.");
    }

    private function waitUntilTopicDeleted(Client $client, string $topicName): void
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

            if (\count($response->topics) === 1 && $response->topics[0]->name === $topicName && $response->topics[0]->errorCode !== 0) {
                return;
            }

            usleep(200_000);
        }

        Assert::fail("Topic '{$topicName}' did not disappear from metadata.");
    }

    /**
     * @param list<non-empty-string> $requestedTopics
     * @param list<non-empty-string> $expectedTopics
     */
    private function waitUntilClusterMetadataContainsTopics(
        Cluster $cluster,
        array $requestedTopics,
        array $expectedTopics,
    ): Cluster\Metadata {
        for ($attempt = 0; $attempt < 10; ++$attempt) {
            $metadata = $cluster->refreshMetadata($requestedTopics);

            if (array_keys($metadata->topics) === $expectedTopics) {
                return $metadata;
            }

            usleep(200_000);
        }

        Assert::fail(
            \sprintf(
                'Cluster metadata did not contain expected topics: expected [%s], got [%s].',
                implode(', ', $expectedTopics),
                implode(', ', array_keys($metadata->topics ?? [])),
            ),
        );
    }
}
