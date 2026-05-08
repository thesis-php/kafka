<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated;

final class AdminClientTest extends TestCase
{
    #[Test]
    public function apiVersions(): void
    {
        $client = $this->createClient();

        $response = $client->request(new Generated\ApiVersionsRequest(
            clientSoftwareName: 'thesis-kafka-tests',
            clientSoftwareVersion: 'dev',
        ));

        Assert::same($response->errorCode, 0);
        Assert::true(\count($response->apiKeys) > 0);

        $client->close();
    }

    #[Test]
    public function describeCluster(): void
    {
        $client = $this->createClient();

        $response = $client->request(new Generated\DescribeClusterRequest(
            includeClusterAuthorizedOperations: false,
            endpointType: 1,
            includeFencedBrokers: false,
        ));

        Assert::same($response->errorCode, 0);
        Assert::same($response->clusterId, self::CLUSTER_ID);
        Assert::count($response->brokers, 3);
        Assert::same(
            array_map(
                static fn(Generated\DescribeClusterResponse\DescribeClusterBroker $broker) => "{$broker->host}:{$broker->port}",
                $response->brokers,
            ),
            self::SEEDS,
        );

        $client->close();
    }

    #[Test]
    public function metadata(): void
    {
        $client = $this->createClient();

        $response = $client->request(new Generated\MetadataRequest(
            topics: null,
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        ));

        Assert::same($response->errorCode, 0);
        Assert::same($response->clusterId, self::CLUSTER_ID);
        Assert::count($response->brokers, 3);

        $client->close();
    }

    #[Test]
    public function createTopic(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest(
            client: $client,
            numPartitions: 3,
            replicationFactor: 2,
            configs: [
                new Generated\CreateTopicsRequest\CreatableTopic\CreatableTopicConfig(
                    name: 'cleanup.policy',
                    value: 'compact',
                ),
            ],
        );

        Assert::same($topic->numPartitions, 3);
        Assert::same($topic->replicationFactor, 2);
        Assert::same($topic->errorCode, 0);

        $this->deleteTopicRequest($client, $topic->name);

        $client->close();
    }

    #[Test]
    public function deleteTopic(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest($client);

        Assert::same($topic->errorCode, 0);

        $deleted = $this->deleteTopicRequest($client, $topic->name);

        Assert::count($deleted->responses, 1);
        \assert(\count($deleted->responses) > 0);
        Assert::same($deleted->responses[0]->name, $topic->name);
        Assert::same($deleted->responses[0]->errorCode, 0);

        $client->close();
    }

    #[Test]
    public function createTopicValidateOnly(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest($client, validateOnly: true);

        Assert::same($topic->errorCode, 0);

        $client->close();
    }

    #[Test]
    public function metadataForCreatedTopic(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest($client);

        $response = $client->request(new Generated\MetadataRequest(
            topics: [
                new Generated\MetadataRequest\MetadataRequestTopic(
                    topicId: Uuid::fromString(Uuid::NIL),
                    name: $topic->name,
                ),
            ],
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        ));

        Assert::count($response->topics, 1);
        \assert(\count($response->topics) > 0);
        Assert::same($response->topics[0]->name, $topic->name);
        Assert::same($response->topics[0]->errorCode, 0);

        $this->deleteTopicRequest($client, $topic->name);

        $client->close();
    }

    #[Test]
    public function describeConfigsForCreatedTopic(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest($client);
        $response = $this->waitUntilTopicConfigsVisible($client, $topic->name);

        Assert::count($response->results, 1);
        \assert(\count($response->results) > 0);
        Assert::same($response->results[0]->resourceType, 2);
        Assert::same($response->results[0]->resourceName, $topic->name);
        Assert::same($response->results[0]->errorCode, 0);

        $this->deleteTopicRequest($client, $topic->name);

        $client->close();
    }

    #[Test]
    public function createPartitions(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopicRequest($client);

        $response = $client->request(new Generated\CreatePartitionsRequest(
            topics: [
                new Generated\CreatePartitionsRequest\CreatePartitionsTopic(
                    name: $topic->name,
                    count: 3,
                    assignments: null,
                ),
            ],
            timeoutMs: 10_000,
            validateOnly: false,
        ));

        Assert::count($response->results, 1);
        \assert(\count($response->results) > 0);
        Assert::same($response->results[0]->name, $topic->name);
        Assert::same($response->results[0]->errorCode, 0);

        $this->deleteTopicRequest($client, $topic->name);

        $client->close();
    }

    /**
     * @param list<Generated\CreateTopicsRequest\CreatableTopic\CreatableTopicConfig> $configs
     */
    private function createTopicRequest(
        Client $client,
        int $numPartitions = 1,
        int $replicationFactor = 1,
        bool $validateOnly = false,
        array $configs = [],
    ): Generated\CreateTopicsResponse\CreatableTopicResult {
        $topicName = $this->topicName();

        $response = $client->request(new Generated\CreateTopicsRequest(
            topics: [
                new Generated\CreateTopicsRequest\CreatableTopic(
                    name: $topicName,
                    numPartitions: $numPartitions,
                    replicationFactor: $replicationFactor,
                    assignments: [],
                    configs: $configs,
                ),
            ],
            timeoutMs: 10_000,
            validateOnly: $validateOnly,
        ));

        \assert(\count($response->topics) > 0);

        return $response->topics[0];
    }

    private function deleteTopicRequest(
        Client $client,
        string $topicName,
    ): Generated\DeleteTopicsResponse {
        return $client->request(new Generated\DeleteTopicsRequest(
            topics: [
                new Generated\DeleteTopicsRequest\DeleteTopicState(
                    name: $topicName,
                    topicId: Uuid::fromString(Uuid::NIL),
                ),
            ],
            topicNames: [$topicName],
            timeoutMs: 10_000,
        ));
    }

    private function waitUntilTopicConfigsVisible(
        Client $client,
        string $topicName,
    ): Generated\DescribeConfigsResponse {
        for ($attempt = 0; $attempt < 10; ++$attempt) {
            $response = $client->request(new Generated\DescribeConfigsRequest(
                resources: [
                    new Generated\DescribeConfigsRequest\DescribeConfigsResource(
                        resourceType: 2,
                        resourceName: $topicName,
                        configurationKeys: null,
                    ),
                ],
                includeSynonyms: false,
                includeDocumentation: false,
            ));

            if (
                \count($response->results) === 1
                && $response->results[0]->resourceName === $topicName
                && $response->results[0]->errorCode === 0
            ) {
                return $response;
            }

            usleep(200_000);
        }

        Assert::fail("Topic '{$topicName}' configs did not become visible.");
    }
}
