<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;

/**
 * The fetch core shared by every consumer kind (manual, group). Given a concrete set of
 * (topic, partition) → start offset, it spins up one fetch loop per leader ({@see FetchCycle})
 * and decodes the batches. It gives each batch to the owner. An error of the fetch that cannot
 * be fixed goes to the owner too. It does not decide which partitions to read — callers
 * ({@see DirectConsumer}, {@see GroupConsumer}) resolve that and hand it in.
 *
 * {@see AssignmentConsumer::stop()} tears down the current fetch. A subsequent
 * {@see AssignmentConsumer::start()} with a different assignment starts a fresh cycle:
 * this is how a group consumer re-fetches after a rebalance.
 *
 * @internal
 */
final class AssignmentConsumer
{
    private readonly Protocol\RecordBatch $batch;

    private ?FetchCycle $cycle = null;

    /**
     * @param TimeSpan $requestTimeout the request timeout of the client
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Consumer\Config $config,
        private readonly OffsetLister $offsets,
        private readonly TimeSpan $requestTimeout,
        private readonly ContextLogger $logger,
    ) {
        $this->batch = new Protocol\RecordBatch(
            decompressors: $config->decompressors,
            crc32c: $config->crc32,
        );
    }

    /**
     * Begin fetching the given partitions. Call {@see stop()} before the next start.
     * Nothing runs when this method throws.
     *
     * @param TopicPartition\Map<non-negative-int> $assignments
     * @param \Closure(non-empty-list<Consumer\Record>, Cancellation): void $deliver gets each batch, it waits while the buffer is full
     * @param \Closure(\Throwable): void $failed gets the error that stopped the fetch
     * @throws Exception
     * @throws CancelledException
     */
    public function start(
        TopicPartition\Map $assignments,
        \Closure $deliver,
        \Closure $failed,
        Cancellation $cancellation,
    ): void {
        \assert($this->cycle === null);

        $subscription = $this->createSubscription($assignments, $cancellation);

        $this->cycle = new FetchCycle(
            cluster: $this->cluster,
            subscription: $subscription,
            offsets: $this->offsets,
            batch: $this->batch,
            config: $this->config,
            requestTimeout: $this->requestTimeout,
            deliver: $deliver,
            failed: $failed,
            logger: $this->logger,
        );

        $this->cycle->start();
    }

    /**
     * Stop the fetch entirely. No batch is delivered after this method returns.
     * After this a new {@see start()} can begin a different assignment.
     */
    public function stop(): void
    {
        // The wait below lets another coroutine call start(). Clear the state first,
        // so this stop() does not touch the new cycle.
        $cycle = $this->cycle;
        $this->cycle = null;

        $cycle?->stop();
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $assignments
     * @throws Exception
     */
    private function createSubscription(
        TopicPartition\Map $assignments,
        Cancellation $cancellation,
    ): SubscriptionState {
        /** @var ?UuidInterface $topicNilId */
        static $topicNilId = null;
        $topicNilId ??= Uuid::fromString(Uuid::NIL);

        $topics = $assignments->topics();
        \assert(\count($topics) > 0);

        // Resolve which partitions exist and who leads them.
        $metadata = $this->cluster->lookup(
            $topics,
            $cancellation,
        );

        $subscription = new SubscriptionState($topics);

        foreach ($assignments as $tp) {
            $topic = $tp->topic;
            $partition = $tp->partition;

            $topicMetadata = $metadata->topics[$topic]
                ?? throw new Exception\UnknownTopic($topic);

            if (!$topicMetadata->topicId->equals($topicNilId)) {
                $subscription->mapTopicId($topicMetadata->topicId, $topic);
            }

            /** @var -1|non-negative-int $leader */
            $leader = $topicMetadata->partitions[$partition]->leader ?? -1;
            if ($leader === -1) {
                throw new Exception\NoAvailablePartitionLeader($topic, $partition);
            }

            $subscription->assign($tp, $assignments[$tp], $leader);
        }

        return $subscription;
    }
}
