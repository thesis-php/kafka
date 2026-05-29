<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Kafka\Config;
use Thesis\Kafka\Internal\Cluster\Metadata\TopicKind;
use Thesis\Kafka\TestCase;

#[Covers(Cluster::class)]
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
        $preloadedTopic = $this->createTopic($client, prefix: 'thesis-preload');
        $requestedTopic = $this->createTopic($client, prefix: 'thesis-request');
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
        $metadata = $this->waitUntilClusterMetadataDropsTopics(
            $cluster,
            [$topic],
            [$topic],
        );

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

    /**
     * @param list<non-empty-string> $requestedTopics
     * @param list<non-empty-string> $deletedTopics
     */
    private function waitUntilClusterMetadataDropsTopics(
        Cluster $cluster,
        array $requestedTopics,
        array $deletedTopics,
    ): Cluster\Metadata {
        for ($attempt = 0; $attempt < 10; ++$attempt) {
            $metadata = $cluster->refreshMetadata($requestedTopics);

            if (array_intersect(array_keys($metadata->topics), $deletedTopics) === []) {
                return $metadata;
            }

            usleep(200_000);
        }

        Assert::fail(
            \sprintf(
                'Cluster metadata still contained deleted topics: [%s].',
                implode(', ', array_keys($metadata->topics ?? [])),
            ),
        );
    }
}
