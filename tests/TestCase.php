<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Ramsey\Uuid\Uuid;
use Testo\Assert;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Protocol\Generated;

abstract readonly class TestCase
{
    /** @var non-empty-list<non-empty-string> */
    final public const array SEEDS = [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ];
    final public const string CLUSTER_ID = '4L6g3nShT-eMCtK--X86sw';

    /**
     * @param non-empty-list<non-empty-string> $seeds
     */
    final protected function createClient(
        array $seeds = self::SEEDS,
        ?SASLConfig $sasl = null,
    ): Client {
        return new Client(new Config(
            seeds: $seeds,
            sasl: $sasl,
        ));
    }

    /**
     * @return non-empty-string
     */
    final protected function topicName(string $prefix = 'thesis-it'): string
    {
        return \sprintf('%s-%s', $prefix, bin2hex(random_bytes(6)));
    }

    final protected function metadataRequest(): Generated\MetadataRequest
    {
        return new Generated\MetadataRequest(
            topics: null,
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        );
    }

    /**
     * @return non-empty-string
     */
    final protected function createTopic(
        Client $client,
        string $prefix = 'thesis-it',
        int $partitions = 1,
    ): string {
        $topicName = $this->topicName($prefix);

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

    final protected function deleteTopic(Client $client, string $topicName): void
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

    final protected function waitUntilTopicVisible(
        Client $client,
        string $topicName,
        int $partitions = 1,
    ): Generated\MetadataResponse {
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
                && \count($response->topics[0]->partitions) >= $partitions
                && array_filter(
                    \array_slice($response->topics[0]->partitions, 0, $partitions),
                    static fn(Generated\MetadataResponse\MetadataResponseTopic\MetadataResponsePartition $partition) => $partition->errorCode === 0 && $partition->leaderId >= 0,
                ) === \array_slice($response->topics[0]->partitions, 0, $partitions)
            ) {
                return $response;
            }

            usleep(200_000);
        }

        Assert::fail("Topic '{$topicName}' did not become visible in metadata.");
    }
}
