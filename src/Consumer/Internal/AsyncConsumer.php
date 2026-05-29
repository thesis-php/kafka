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
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final class AsyncConsumer implements Consumer
{
    /** @var Pipeline\Queue<non-empty-list<Consumer\Record>> */
    private readonly Pipeline\Queue $queue;

    /** @var Pipeline\ConcurrentIterator<non-empty-list<Consumer\Record>> */
    private readonly Pipeline\ConcurrentIterator $iterator;

    private readonly SubscriptionState $subscription;

    private readonly Protocol\RecordBatch $batch;

    /** @var array<int, Fetcher> */
    private array $fetchers = [];

    private AsyncConsumer\ConsumerState $consumer = AsyncConsumer\ConsumerState::Idle;

    private AsyncConsumer\ConsumeState $consume = AsyncConsumer\ConsumeState::Ready;

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Consumer\Config $config,
        private readonly TimeSpan $requestTimeout,
        array $topics,
    ) {
        $this->queue = new Pipeline\Queue($config->prefetchBufferSize);
        $this->iterator = $this->queue->iterate();
        $this->subscription = new SubscriptionState($topics);
        $this->batch = new Protocol\RecordBatch(
            decompressors: $config->decompressors,
            crc32c: $config->crc32,
        );
    }

    #[\Override]
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->consumer === AsyncConsumer\ConsumerState::Idle) {
            $this->consumer = AsyncConsumer\ConsumerState::Running;
            $this->run($cancellation);
        } elseif ($this->consumer === AsyncConsumer\ConsumerState::Closed) {
            throw new Exception\ConsumerClosed();
        }

        if ($this->consume === AsyncConsumer\ConsumeState::Running) {
            throw new Exception\ConsumerBusy();
        }

        $this->consume = AsyncConsumer\ConsumeState::Running;

        if ($cancellation === null && $mode === Consumer\ConsumeMode::Poll) {
            $cancellation = new TimeoutCancellation(
                $this->config->pollTimeout->toSeconds(3),
            );
        }

        $cancellation ??= new NullCancellation();

        return $this->createIterator($mode, $cancellation);
    }

    #[\Override]
    public function commit(
        Consumer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): void {
        throw new \RuntimeException('Not implemented yet');
    }

    #[\Override]
    public function close(): void
    {
        if ($this->consumer !== AsyncConsumer\ConsumerState::Running) {
            return;
        }

        $this->consumer = AsyncConsumer\ConsumerState::Closed;
        $this->queue->complete();

        $fetchers = $this->fetchers;
        $this->fetchers = [];

        $futures = array_map(
            static fn(Fetcher $fetcher) => async($fetcher->stop(...)),
            array_values($fetchers),
        );

        Future\awaitAll($futures);
    }

    /**
     * @return iterable<non-empty-list<Consumer\Record>>
     */
    private function createIterator(
        Consumer\ConsumeMode $mode,
        Cancellation $cancellation,
    ): iterable {
        try {
            if (!$this->iterator->continue($cancellation)) {
                return;
            }

            yield $this->iterator->getValue();

            if ($mode === Consumer\ConsumeMode::Stream) {
                while ($this->iterator->continue($cancellation)) {
                    yield $this->iterator->getValue();
                }
            }
        } catch (CancelledException) {
            return;
        } finally {
            $this->consume = AsyncConsumer\ConsumeState::Ready;
        }
    }

    /**
     * @throws Exception
     */
    private function run(?Cancellation $cancellation = null): void
    {
        /** @var ?UuidInterface $topicNilId */
        static $topicNilId = null;
        $topicNilId ??= Uuid::fromString(Uuid::NIL);

        // Resolve which partitions exist and who leads them.
        $metadata = $this->cluster->refreshMetadata(
            $this->subscription->topics,
            $cancellation ?? new NullCancellation(),
        );

        // Resolve the starting offset of every partition per the
        // autoOffsetReset policy (ListOffsets under the hood).
        $offsets = $this->resolveOffsets($metadata, $cancellation);

        foreach ($this->subscription->topics as $topic) {
            $topicMetadata = $metadata->topics[$topic]
                ?? throw new Exception\UnknownTopic($topic);

            if (!$topicMetadata->topicId->equals($topicNilId)) {
                $this->subscription->mapTopicId($topicMetadata->topicId, $topic);
            }

            /** @var non-negative-int $partition */
            foreach ($topicMetadata->partitions as $partition => $partitionMetadata) {
                /** @var -1|non-negative-int $leader */
                $leader = $partitionMetadata->leader;

                if ($leader === -1) {
                    throw new Exception\NoAvailablePartitionLeader($topic, $partition);
                }

                $tp = new TopicPartition($topic, $partition);

                $this->subscription->assign(
                    $tp,
                    $offsets[$tp],
                    $leader,
                );
            }
        }

        foreach ($this->subscription->leaders() as $leaderId) {
            $fetcher = new Fetcher(
                leaderId: $leaderId,
                cluster: $this->cluster,
                subscription: $this->subscription,
                batch: $this->batch,
                config: $this->config,
                queue: $this->queue,
            );

            $this->fetchers[$leaderId] = $fetcher;
            $fetcher->start();
        }
    }

    /**
     * @return TopicPartition\Map<non-negative-int>
     * @throws Exception
     */
    private function resolveOffsets(
        Cluster\Metadata $metadata,
        ?Cancellation $cancellation = null,
    ): TopicPartition\Map {
        /** @var array<int, list<TopicPartition>> $byLeader */
        $byLeader = [];
        foreach ($this->subscription->topics as $topic) {
            $topicMetadata = $metadata->topics[$topic]
                ?? throw new Exception\UnknownTopic($topic);

            /** @var non-negative-int $partition */
            foreach ($topicMetadata->partitions as $partition => $partitionMetadata) {
                if ($partitionMetadata->leader === -1) {
                    throw new Exception\NoAvailablePartitionLeader($topic, $partition);
                }

                $byLeader[$partitionMetadata->leader][] = new TopicPartition(
                    $topic,
                    $partition,
                );
            }
        }

        $futures = [];
        foreach ($byLeader as $leaderId => $partitions) {
            $futures[$leaderId] = async(
                $this->listOffsets(...),
                $leaderId,
                $partitions,
                $cancellation,
            );
        }

        /** @var array<int, TopicPartition\Map<non-negative-int>> $perLeader */
        $perLeader = Future\await($futures, $cancellation);

        /** @var TopicPartition\Map<non-negative-int> $result */
        $result = new TopicPartition\Map();
        foreach ($perLeader as $leaderResult) {
            foreach ($leaderResult as $tp) {
                $result[$tp] = $leaderResult[$tp];
            }
        }

        return $result;
    }

    /**
     * @param non-negative-int $leaderId
     * @param non-empty-list<TopicPartition> $partitions
     * @return TopicPartition\Map<non-negative-int>
     * @throws Exception
     * @throws CancelledException
     */
    private function listOffsets(
        int $leaderId,
        array $partitions,
        ?Cancellation $cancellation = null,
    ): TopicPartition\Map {
        /** @var array<non-empty-string, list<Generated\ListOffsetsRequest\ListOffsetsTopic\ListOffsetsPartition>> $partitionsByTopic */
        $partitionsByTopic = [];

        foreach ($partitions as $tp) {
            $partitionsByTopic[$tp->topic][] = new Generated\ListOffsetsRequest\ListOffsetsTopic\ListOffsetsPartition(
                partitionIndex: $tp->partition,
                currentLeaderEpoch: -1,
                timestamp: $this->config->autoOffsetReset->value,
            );
        }

        $topics = [];
        foreach ($partitionsByTopic as $topic => $listPartitions) {
            $topics[] = new Generated\ListOffsetsRequest\ListOffsetsTopic(
                name: $topic,
                partitions: $listPartitions,
            );
        }

        $request = new Generated\ListOffsetsRequest(
            replicaId: -1, // -1 = consumer, not a replica
            isolationLevel: $this->config->isolationLevel->value,
            topics: $topics,
            timeoutMs: (int) $this->requestTimeout->toMilliseconds(3),
        );

        $response = $this->cluster->request(
            $request,
            $cancellation,
            brokerId: $leaderId,
        );

        /** @var TopicPartition\Map<non-negative-int> $result */
        $result = new TopicPartition\Map();

        foreach ($response->topics as $topicResponse) {
            foreach ($topicResponse->partitions as $partitionResponse) {
                $error = ErrorCode::from($partitionResponse->errorCode);
                if ($error !== ErrorCode::None) {
                    throw new \RuntimeException(
                        "ListOffsets failed for {$topicResponse->name}-{$partitionResponse->partitionIndex}: {$error->name}.",
                    );
                }

                $topic = $topicResponse->name;
                \assert($topic !== '');

                $partition = $partitionResponse->partitionIndex;
                \assert($partition >= 0);

                $tp = new TopicPartition(
                    $topic,
                    $partition,
                );

                $offset = $partitionResponse->offset;
                \assert($offset >= 0);

                $result[$tp] = $offset;
            }
        }

        return $result;
    }
}
