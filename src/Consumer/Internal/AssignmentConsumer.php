<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\TimeoutCancellation;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\TopicPartition;
use function Amp\async;

/**
 * The fetch core shared by every consumer kind (manual, group). Given a concrete set of
 * (topic, partition) → start offset, it spins up one fetch loop per leader,
 * decodes the batches, and yields them through {@see AssignmentConsumer::consume()}. It does not decide
 * which partitions to read — callers ({@see DirectConsumer}, {@see GroupConsumer}) resolve
 * that and hand it in.
 *
 * One {@see AssignmentConsumer::consume()} at a time. {@see AssignmentConsumer::stop()} tears down the current fetch
 * (loops, queue, iterator). A subsequent {@see AssignmentConsumer::consume()} with a different assignment
 * starts a fresh cycle: this is how a group consumer re-fetches after a rebalance.
 *
 * @internal
 */
final class AssignmentConsumer
{
    /** @var ?Pipeline\Queue<non-empty-list<Consumer\Record>> */
    private ?Pipeline\Queue $queue = null;

    /** @var ?Pipeline\ConcurrentIterator<non-empty-list<Consumer\Record>> */
    private ?Pipeline\ConcurrentIterator $iterator = null;

    private ?SubscriptionState $subscription = null;

    private readonly Protocol\RecordBatch $batch;

    /** @var array<int, Fetcher> */
    private array $fetchers = [];

    private AssignmentConsumer\ConsumerState $consumer = AssignmentConsumer\ConsumerState::Idle;

    private AssignmentConsumer\ConsumeState $consume = AssignmentConsumer\ConsumeState::Idle;

    public function __construct(
        private readonly Cluster $cluster,
        private readonly Consumer\Config $config,
        private readonly ContextLogger $logger,
    ) {
        $this->batch = new Protocol\RecordBatch(
            decompressors: $config->decompressors,
            crc32c: $config->crc32,
        );
    }

    /**
     * Begin fetching the given partitions. Starts the leader loops and the
     * buffer. Does not read. Idempotent per assignment: call {@see stop()} to rebalance.
     *
     * @param TopicPartition\Map<non-negative-int> $assignments
     * @throws Exception
     */
    public function start(
        TopicPartition\Map $assignments,
        ?Cancellation $cancellation = null,
    ): void {
        if ($this->consumer === AssignmentConsumer\ConsumerState::Running) {
            throw new \LogicException('Consumer already running.');
        }

        $this->consumer = AssignmentConsumer\ConsumerState::Running;

        $this->queue = new Pipeline\Queue();
        $this->iterator = $this->queue->iterate();

        $this->subscription = $this->createSubscription(
            $assignments,
            $cancellation,
        );

        $this->runFetch(
            $this->subscription,
            $this->queue,
        );
    }

    /**
     * @return iterable<non-empty-list<Consumer\Record>>
     * @throws Exception
     */
    public function consume(
        Consumer\ConsumeMode $mode,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->consume === AssignmentConsumer\ConsumeState::Running) {
            throw new Exception\ConsumerBusy();
        }

        if ($this->iterator === null) {
            throw new \LogicException('Consumer is not running.');
        }

        $this->consume = AssignmentConsumer\ConsumeState::Running;

        if ($cancellation === null && $mode === Consumer\ConsumeMode::Poll) {
            $cancellation = new TimeoutCancellation(
                $this->config->pollTimeout->toSeconds(3),
            );
        }

        $cancellation ??= new NullCancellation();

        return $this->iterate(
            $this->iterator,
            $mode,
            $cancellation,
        );
    }

    /**
     * Stop the fetch entirely: leader loops, buffer, state. After this a
     * new {@see start()} can begin a different assignment.
     */
    public function stop(): void
    {
        if ($this->consumer !== AssignmentConsumer\ConsumerState::Running) {
            return;
        }

        $this->consumer = AssignmentConsumer\ConsumerState::Idle;

        $this->queue?->complete();

        $fetchers = $this->fetchers;
        $this->fetchers = [];

        $futures = array_map(
            static fn(Fetcher $fetcher) => async($fetcher->stop(...)),
            array_values($fetchers),
        );

        Future\awaitAll($futures);

        $this->queue = null;
        $this->iterator = null;
        $this->subscription = null;
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $assignments
     * @throws Exception
     */
    private function createSubscription(
        TopicPartition\Map $assignments,
        ?Cancellation $cancellation = null,
    ): SubscriptionState {
        /** @var ?UuidInterface $topicNilId */
        static $topicNilId = null;
        $topicNilId ??= Uuid::fromString(Uuid::NIL);

        $topics = $assignments->topics();
        \assert(\count($topics) > 0);

        // Resolve which partitions exist and who leads them.
        $metadata = $this->cluster->refreshMetadata(
            $topics,
            $cancellation ?? new NullCancellation(),
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

    /**
     * @param Pipeline\Queue<non-empty-list<Consumer\Record>> $queue
     */
    private function runFetch(
        SubscriptionState $subscription,
        Pipeline\Queue $queue,
    ): void {
        foreach ($subscription->leaders() as $leaderId) {
            $fetcher = new Fetcher(
                leaderId: $leaderId,
                cluster: $this->cluster,
                subscription: $subscription,
                batch: $this->batch,
                config: $this->config,
                queue: $queue,
                // @infection-ignore-all
                logger: $this->logger->with([
                    'component' => 'consumer.fetcher',
                    'nodeId' => $leaderId,
                ]),
            );

            $this->fetchers[$leaderId] = $fetcher;
            $fetcher->start();
        }
    }

    /**
     * @param Pipeline\ConcurrentIterator<non-empty-list<Consumer\Record>> $iterator
     * @return iterable<non-empty-list<Consumer\Record>>
     */
    private function iterate(
        Pipeline\ConcurrentIterator $iterator,
        Consumer\ConsumeMode $mode,
        Cancellation $cancellation,
    ): iterable {
        try {
            if (!$iterator->continue($cancellation)) {
                return;
            }

            yield $iterator->getValue();

            if ($mode === Consumer\ConsumeMode::Stream) {
                while ($iterator->continue($cancellation)) {
                    yield $iterator->getValue();
                }
            }
        } catch (CancelledException) {
            return;
        } finally {
            $this->consume = AssignmentConsumer\ConsumeState::Idle;
        }
    }
}
