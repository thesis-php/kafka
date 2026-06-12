<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\TopicPartition;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use function Amp\async;

/**
 * @internal
 */
final class Fetcher
{
    /** @var ?Future<null> */
    private ?Future $loop = null;

    private ?DeferredCancellation $cancel = null;

    /**
     * @param non-negative-int $leaderId
     * @param Pipeline\Queue<non-empty-list<Consumer\Record>> $queue
     */
    public function __construct(
        private readonly int $leaderId,
        private readonly Cluster $cluster,
        private readonly SubscriptionState $subscription,
        private readonly Consumer\Internal\Protocol\RecordBatch $batch,
        private readonly Consumer\Config $config,
        private readonly Pipeline\Queue $queue,
        private readonly ContextLogger $logger,
    ) {}

    public function start(): void
    {
        $this->cancel ??= new DeferredCancellation();
        $this->loop ??= async($this->run(...), $this->cancel->getCancellation());

        $this->logger->debug('fetch loop started');
    }

    public function stop(Cancellation $cancellation = new NullCancellation()): void
    {
        try {
            // Signal the loop to unwind: any in-flight fetch / queue push
            // sees this cancellation and throws CancelledException, which
            // run() catches and exits on.
            $this->cancel?->cancel();

            // Wait for the loop to actually finish unwinding. await() here
            // (not ignore()) means stop() returns only once the loop has
            // released its resources.
            $this->loop?->await($cancellation);
        } catch (CancelledException) {
            // Expected: cancellation is how we asked it to stop.
        } finally {
            $this->cancel = null;
            $this->loop = null;

            $this->logger->debug('fetch loop stopped');
        }
    }

    private function run(Cancellation $cancellation): void
    {
        while (!$cancellation->isRequested()) {
            $partitions = $this->subscription->partitionsForLeader($this->leaderId);
            if (\count($partitions) === 0) {
                // No partitions led by this broker anymore — nothing to do.
                return;
            }

            try {
                $response = $this->fetch($partitions, $cancellation);
            } catch (CancelledException $e) {
                throw $e;
            } catch (\Throwable $exception) {
                $this->logger->warning('fetch loop error, refreshing metadata', [
                    'exception' => $exception,
                ]);

                // Transport-level failure or a leader that moved.
                // Refresh so the consumer can re-resolve leaders, then
                // retry the loop.
                $this->cluster->refreshMetadata($this->subscription->topics, $cancellation);
                continue;
            }

            $this->dispatch($response, $cancellation);
        }
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $partitions
     * @throws Exception
     * @throws CancelledException
     */
    private function fetch(
        TopicPartition\Map $partitions,
        Cancellation $cancellation,
    ): Generated\FetchResponse {
        $request = $this->createFetchRequest($partitions, $cancellation);

        $this->logger->debug('fetching', [
            'maxBytes' => $request->maxBytes,
            'maxWaitMs' => $request->maxWaitMs,
        ]);

        $response = $this->cluster->request(
            $request,
            $cancellation,
            brokerId: $this->leaderId,
        );

        $batchCount = 0;
        foreach ($response->responses as $topicResponse) {
            foreach ($topicResponse->partitions as $partitionResponse) {
                if ($partitionResponse->records !== null && $partitionResponse->records !== '') {
                    ++$batchCount;
                }
            }
        }

        $this->logger->debug('fetch returned', [
            'batches' => $batchCount,
        ]);

        return $response;
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $partitions
     * @throws Exception
     */
    private function createFetchRequest(
        TopicPartition\Map $partitions,
        Cancellation $cancellation,
    ): Generated\FetchRequest {
        /** @var ?UuidInterface $replicaDirectoryId */
        static $replicaDirectoryId = null;
        $replicaDirectoryId ??= Uuid::fromString(Uuid::NIL);

        $metadata = $this->cluster->metadata($cancellation);

        // FetchRequest nests partitions under topics, so group the flat
        // (topic, partition) → offset map by topic first.
        /** @var array<non-empty-string, list<Generated\FetchRequest\FetchTopic\FetchPartition>> $partitionsByTopic */
        $partitionsByTopic = [];

        foreach ($partitions as $tp) {
            $partitionsByTopic[$tp->topic][] = new Generated\FetchRequest\FetchTopic\FetchPartition(
                partition: $tp->partition,
                currentLeaderEpoch: -1,
                fetchOffset: $partitions[$tp],
                lastFetchedEpoch: -1,
                logStartOffset: -1,
                partitionMaxBytes: $this->config->maxPartitionBytes,
                replicaDirectoryId: $replicaDirectoryId,
                highWatermark: -1,
            );
        }

        $topics = [];
        foreach ($partitionsByTopic as $topic => $fetchPartitions) {
            $meta = $metadata->topics[$topic]
                ?? throw new Exception\UnknownTopic($topic);

            $topics[] = new Generated\FetchRequest\FetchTopic(
                topic: $topic,
                topicId: $meta->topicId,
                partitions: $fetchPartitions,
            );
        }

        return new Generated\FetchRequest(
            clusterId: null,
            replicaId: -1, // -1 = regular consumer, not a replica
            replicaState: new Generated\FetchRequest\ReplicaState(
                replicaId: -1,
                replicaEpoch: -1,
            ),
            maxWaitMs: $this->config->maxWait->toMilliseconds(),
            minBytes: $this->config->minBytes,
            maxBytes: $this->config->maxBytes,
            isolationLevel: $this->config->isolationLevel->value,
            sessionId: 0,
            sessionEpoch: -1,
            topics: $topics,
            forgottenTopicsData: [],
            rackId: '',
        );
    }

    private function dispatch(
        Generated\FetchResponse $response,
        Cancellation $cancellation,
    ): void {
        foreach ($response->responses as $topicResponse) {
            $topic = $topicResponse->topic;
            if ($topic === '') {
                $topic = $this->subscription->topicsById[$topicResponse->topicId->toString()] ?? null;
            }

            if ($topic === null) {
                // Broker returned a topicId we don't have in metadata — could
                // be a topic that vanished between our metadata refresh and
                // this fetch.
                $this->logger->warning('received records for unknown topic id, skipping', [
                    'topicId' => $topicResponse->topicId->toString(),
                ]);

                continue;
            }

            foreach ($topicResponse->partitions as $partitionResponse) {
                $error = ErrorCode::from($partitionResponse->errorCode);
                if ($error !== ErrorCode::None) {
                    $this->logger->warning('fetch error for partition', [
                        'topic' => $topic,
                        'partition' => $partitionResponse->partitionIndex,
                        'error' => $error->name,
                    ]);

                    continue;
                }

                $partition = $partitionResponse->partitionIndex;
                \assert($partition >= 0);

                $records = $partitionResponse->records;

                // A null/empty blob means the long-poll returned with no
                // new data for this partition.
                if ($records === null || $records === '') {
                    continue;
                }

                $this->dispatchRecords(
                    new TopicPartition($topic, $partition),
                    $records,
                    $cancellation,
                );
            }
        }
    }

    /**
     * @param non-empty-string $blob
     * @throws Exception
     */
    private function dispatchRecords(
        TopicPartition $tp,
        string $blob,
        Cancellation $cancellation,
    ): void {
        $batches = $this->batch->decode(
            $blob,
            $tp->topic,
            $tp->partition,
        );

        if ($batches === []) {
            return;
        }

        $offset = null;

        foreach ($batches as $records) {
            // Even a control batch (no user records after filtering)
            // occupies offsets in the log, so the position must step past
            // it — otherwise the next fetch returns the same batch again.
            $offset = $records->nextOffset;

            if ($records->records === []) {
                continue; // control batch — nothing to hand to the consumer
            }

            // Backpressure: if the buffer is full this blocks until the
            // consumer drains it (or stop() cancels us).
            $this->queue->pushAsync($records->records)->await($cancellation);
            $this->logger->debug('dispatched partition records', [
                'topic' => $tp->topic,
                'partition' => $tp->partition,
                'records' => \count($records->records),
                'nextOffset' => $records->nextOffset,
            ]);
        }

        $this->subscription->advance($tp, $offset);

        $this->logger->debug('advance position', [
            'topic' => $tp->topic,
            'partition' => $tp->partition,
            'offset' => $offset,
        ]);
    }
}
