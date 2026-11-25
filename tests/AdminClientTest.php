<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;

final readonly class AdminClientTest
{
    private const string CLUSTER_ID = '4L6g3nShT-eMCtK--X86sw';

    #[Test]
    public function apiVersions(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $response = $kafka->client->request(new Generated\ApiVersionsRequest(
                clientSoftwareName: 'thesis-kafka-tests',
                clientSoftwareVersion: 'dev',
            ));

            Assert::same($response->errorCode, 0);
            Assert::true(\count($response->apiKeys) > 0);
        });
    }

    #[Test]
    public function describeCluster(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $response = $kafka->client->request(new Generated\DescribeClusterRequest(
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
                Kafka::SEEDS,
            );
        });
    }

    #[Test]
    public function metadata(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $response = $kafka->client->request(Kafka::metadataRequest());

            Assert::same($response->errorCode, 0);
            Assert::same($response->clusterId, self::CLUSTER_ID);
            Assert::count($response->brokers, 3);
        });
    }

    #[Test]
    public function createsTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest(
                kafka: $kafka,
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
        });
    }

    #[Test]
    public function deletesTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest($kafka);

            Assert::same($topic->errorCode, 0);

            $deleted = self::deleteTopicRequest($kafka, $topic->name);

            Assert::count($deleted->responses, 1);
            \assert(\count($deleted->responses) > 0);
            Assert::same($deleted->responses[0]->name, $topic->name);
            Assert::same($deleted->responses[0]->errorCode, 0);
        });
    }

    #[Test]
    public function createTopicValidateOnly(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest($kafka, validateOnly: true);

            Assert::same($topic->errorCode, 0);
        });
    }

    #[Test]
    public function metadataForCreatedTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest($kafka);
            $response = $kafka->waitUntilTopicReady($topic->name);

            Assert::count($response->topics, 1);
            \assert(\count($response->topics) > 0);
            Assert::same($response->topics[0]->name, $topic->name);
            Assert::same($response->topics[0]->errorCode, 0);
        });
    }

    #[Test]
    public function describeConfigsForCreatedTopic(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest($kafka);
            $response = $kafka
                ->poll(static fn(Cancellation $cancellation) => $kafka->client->request(
                    new Generated\DescribeConfigsRequest(
                        resources: [
                            new Generated\DescribeConfigsRequest\DescribeConfigsResource(
                                resourceType: 2,
                                resourceName: $topic->name,
                                configurationKeys: null,
                            ),
                        ],
                        includeSynonyms: false,
                        includeDocumentation: false,
                    ),
                    $cancellation,
                ))
                ->expecting("configs of topic '{$topic->name}'")
                ->describedBy(static fn(Generated\DescribeConfigsResponse $response) => implode(', ', array_map(
                    static fn(Generated\DescribeConfigsResponse\DescribeConfigsResult $result) => "{$result->resourceName}: error {$result->errorCode}",
                    $response->results,
                )))
                ->until(static fn(Generated\DescribeConfigsResponse $response) => \count($response->results) === 1
                    && $response->results[0]->resourceName === $topic->name
                    && $response->results[0]->errorCode === 0);

            Assert::count($response->results, 1);
            \assert(\count($response->results) > 0);
            Assert::same($response->results[0]->resourceType, 2);
            Assert::same($response->results[0]->resourceName, $topic->name);
            Assert::same($response->results[0]->errorCode, 0);
        });
    }

    #[Test]
    public function createPartitions(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = self::createTopicRequest($kafka);

            $response = $kafka->client->request(new Generated\CreatePartitionsRequest(
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
        });
    }

    /**
     * @param list<Generated\CreateTopicsRequest\CreatableTopic\CreatableTopicConfig> $configs
     */
    private static function createTopicRequest(
        Kafka $kafka,
        int $numPartitions = 1,
        int $replicationFactor = 1,
        bool $validateOnly = false,
        array $configs = [],
    ): Generated\CreateTopicsResponse\CreatableTopicResult {
        $response = $kafka->client->request(new Generated\CreateTopicsRequest(
            topics: [
                new Generated\CreateTopicsRequest\CreatableTopic(
                    name: $kafka->topicName(),
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

    private static function deleteTopicRequest(
        Kafka $kafka,
        string $topicName,
    ): Generated\DeleteTopicsResponse {
        return $kafka->client->request(new Generated\DeleteTopicsRequest(
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
}
