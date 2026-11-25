<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Internal\Cluster\Metadata\TopicKind;
use Thesis\Kafka\Testing\Kafka;

final readonly class ClusterTest
{
    #[Test]
    public function metadataWithoutTopicsReturnsOnlyBrokers(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $cluster = $kafka->cluster();

            $metadata = $cluster->metadata();

            Assert::array($metadata->brokers)->hasCount(3);
            Assert::array($metadata->topics)->hasCount(0);
            Assert::int($metadata->controllerId)->greaterThanOrEqual(-1);
        });
    }

    #[Test]
    public function metadataWithPreloadTopicsLoadsThem(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $cluster = $kafka->cluster(preloadTopics: [$topic]);
            $metadata = self::waitUntilClusterMetadataContainsTopics(
                $kafka,
                $cluster,
                [],
                [$topic],
            );

            Assert::array($metadata->topics)->hasKeys($topic);
            Assert::same($metadata->topics[$topic]->name ?? null, $topic);
            Assert::same($metadata->topics[$topic]->kind ?? null, TopicKind::User);
        });
    }

    #[Test]
    public function refreshMetadataMergesRequestedTopicsWithPreloadTopics(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $preloadedTopic = $kafka->topic();
            $requestedTopic = $kafka->topic();

            $cluster = $kafka->cluster(preloadTopics: [$preloadedTopic]);
            $metadata = self::waitUntilClusterMetadataContainsTopics(
                $kafka,
                $cluster,
                [$requestedTopic],
                [$preloadedTopic, $requestedTopic],
            );

            Assert::array($metadata->topics)->hasKeys($preloadedTopic, $requestedTopic);
            Assert::same($metadata->topics[$preloadedTopic]->name ?? null, $preloadedTopic);
            Assert::same($metadata->topics[$requestedTopic]->name ?? null, $requestedTopic);
        });
    }

    #[Test]
    public function refreshMetadataDropsDeletedTopics(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();

            $cluster = $kafka->cluster(preloadTopics: [$topic]);
            $metadata = self::waitUntilClusterMetadataContainsTopics(
                $kafka,
                $cluster,
                [],
                [$topic],
            );

            Assert::array($metadata->topics)->hasKeys($topic);

            $kafka->deleteTopic($topic);
            $metadata = $kafka
                ->poll(static fn(Cancellation $cancellation) => $cluster->refreshMetadata([$topic], $cancellation))
                ->expecting(\sprintf('cluster metadata without the deleted topic %s', $topic))
                ->describedBy(self::describeTopics(...))
                ->until(static fn(Cluster\Metadata $metadata) => array_intersect(array_keys($metadata->topics), [$topic]) === []);

            Assert::array($metadata->topics)->hasCount(0);
        });
    }

    /**
     * @param list<non-empty-string> $requestedTopics
     * @param list<non-empty-string> $expectedTopics
     */
    private static function waitUntilClusterMetadataContainsTopics(
        Kafka $kafka,
        Cluster $cluster,
        array $requestedTopics,
        array $expectedTopics,
    ): Cluster\Metadata {
        return $kafka
            ->poll(static fn(Cancellation $cancellation) => $cluster->refreshMetadata($requestedTopics, $cancellation))
            ->expecting(\sprintf('cluster metadata with topics [%s]', implode(', ', $expectedTopics)))
            ->describedBy(self::describeTopics(...))
            ->until(static fn(Cluster\Metadata $metadata) => array_keys($metadata->topics) === $expectedTopics);
    }

    private static function describeTopics(Cluster\Metadata $metadata): string
    {
        return \sprintf('topics [%s]', implode(', ', array_keys($metadata->topics)));
    }
}
