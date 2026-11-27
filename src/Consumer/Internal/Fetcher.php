<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\RequestTimeout;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;

/**
 * The fetch loop of one leader. It reads the partitions that this broker
 * leads in {@see SubscriptionState}. A partition that it cannot read goes
 * to the recovery of {@see FetchCycle}.
 *
 * @internal
 */
final readonly class Fetcher
{
    /**
     * @param non-negative-int $leaderId
     * @param \Closure(non-empty-list<Consumer\Record>, Cancellation): void $deliver gets each batch, it waits while the buffer is full
     * @param TimeSpan $requestTimeout the request timeout of the client
     */
    public function __construct(
        private int $leaderId,
        private Cluster $cluster,
        private SubscriptionState $subscription,
        private FetchCycle $cycle,
        private Consumer\Internal\Protocol\RecordBatch $batch,
        private Consumer\Config $config,
        private TimeSpan $requestTimeout,
        private \Closure $deliver,
        private ContextLogger $logger,
    ) {}

    /**
     * Fetches until this broker leads no partition. Never throws: an error
     * that cannot be fixed stops the whole {@see FetchCycle}.
     */
    public function run(Cancellation $cancellation): void
    {
        try {
            while (true) {
                $partitions = $this->subscription->partitionsForLeader($this->leaderId);
                if (\count($partitions) === 0) {
                    return;
                }

                // The broker can hold the fetch for maxWait. The request timeout is for the network.
                // A connection that was silent for the whole time is closed, so the next attempt uses a new one.
                $limit = RequestTimeout::start(
                    $this->config->maxWait->add($this->requestTimeout)->toSeconds(3),
                    $cancellation,
                );

                $request = $this->createFetchRequest($partitions, $limit);
                if ($request === null) {
                    continue;
                }

                $response = $this->cluster->request(
                    $request,
                    $limit,
                    brokerId: $this->leaderId,
                );

                $this->dispatch($response, $cancellation);
            }
        } catch (\Throwable $e) {
            // The cycle has stopped. The error is a result of the stop.
            if ($cancellation->isRequested()) {
                return;
            }

            if (!FetchCycle::retriable($e)) {
                $this->cycle->fail($e);

                return;
            }

            // @infection-ignore-all
            $this->logger->warning('fetch failed, retrying', [
                'exception' => $e,
            ]);

            foreach ($this->subscription->partitionsForLeader($this->leaderId) as $tp) {
                $this->cycle->retry($tp);
            }
        }
    }

    /**
     * @param TopicPartition\Map<non-negative-int> $partitions
     * @return ?Generated\FetchRequest null when no partition can be fetched now
     * @throws Exception
     */
    private function createFetchRequest(
        TopicPartition\Map $partitions,
        Cancellation $cancellation,
    ): ?Generated\FetchRequest {
        /** @var ?UuidInterface $replicaDirectoryId */
        static $replicaDirectoryId = null;
        $replicaDirectoryId ??= Uuid::fromString(Uuid::NIL);

        $metadata = $this->cluster->metadata($cancellation);

        // FetchRequest nests partitions under topics, so group the flat
        // (topic, partition) → offset map by topic first.
        /** @var array<non-empty-string, list<Generated\FetchRequest\FetchTopic\FetchPartition>> $partitionsByTopic */
        $partitionsByTopic = [];

        /** @var array<non-empty-string, UuidInterface> $topicIds */
        $topicIds = [];

        foreach ($partitions as $tp) {
            // The metadata has lost the topic, for example after a deletion.
            // The recovery finds out if the topic comes back.
            $topicMetadata = $metadata->topics[$tp->topic] ?? null;
            if ($topicMetadata === null) {
                // @infection-ignore-all
                $this->logger->warning('partition fetch failed, retrying', [
                    'topic' => $tp->topic,
                    'partition' => $tp->partition,
                    'offset' => $partitions[$tp],
                    'error' => 'unknown topic',
                ]);

                $this->cycle->retry($tp);

                continue;
            }

            // Send the leader epoch we know. The broker rejects the fetch if
            // this epoch is older than its own. That protects us from a stale leader.
            $leaderEpoch = $topicMetadata->partitions[$tp->partition]->leaderEpoch ?? -1;
            $topicIds[$tp->topic] = $topicMetadata->topicId;

            $partitionsByTopic[$tp->topic][] = new Generated\FetchRequest\FetchTopic\FetchPartition(
                partition: $tp->partition,
                currentLeaderEpoch: $leaderEpoch,
                fetchOffset: $partitions[$tp],
                lastFetchedEpoch: -1,
                logStartOffset: -1,
                partitionMaxBytes: $this->config->maxPartitionBytes,
                replicaDirectoryId: $replicaDirectoryId,
                highWatermark: -1,
            );
        }

        if ($topicIds === []) {
            return null;
        }

        $topics = [];
        foreach ($topicIds as $topic => $topicId) {
            $topics[] = new Generated\FetchRequest\FetchTopic(
                topic: $topic,
                topicId: $topicId,
                partitions: $partitionsByTopic[$topic] ?? [],
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

    /**
     * @throws Exception
     */
    private function dispatch(
        Generated\FetchResponse $response,
        Cancellation $cancellation,
    ): void {
        $error = ErrorCode::tryFrom($response->errorCode) ?? ErrorCode::UnknownServerError;
        if ($error !== ErrorCode::None) {
            throw new Exception\ProtocolError(new Errors([new Error\TopLevelError($error)]));
        }

        foreach ($response->responses as $topicResponse) {
            $topic = $topicResponse->topic;
            if ($topic === '') {
                $topic = $this->subscription->topicsById[$topicResponse->topicId->toString()] ?? null;
            }

            if ($topic === null) {
                // Broker returned a topicId we don't have in metadata — could
                // be a topic that vanished between our metadata refresh and
                // this fetch.
                // @infection-ignore-all
                $this->logger->warning('received records for unknown topic id, skipping', [
                    'topicId' => $topicResponse->topicId->toString(),
                ]);

                continue;
            }

            foreach ($topicResponse->partitions as $partitionResponse) {
                $partition = $partitionResponse->partitionIndex;
                \assert($partition >= 0);

                $tp = new TopicPartition($topic, $partition);

                $error = ErrorCode::tryFrom($partitionResponse->errorCode) ?? ErrorCode::UnknownServerError;
                if ($error !== ErrorCode::None) {
                    // The other partitions of the answer go on.
                    $this->recover($tp, $error);

                    continue;
                }

                $records = $partitionResponse->records;

                // A null/empty blob means the long-poll returned with no
                // new data for this partition.
                if ($records === null || $records === '') {
                    continue;
                }

                $this->dispatchRecords($tp, $records, $cancellation);
            }
        }
    }

    /**
     * @throws Exception
     */
    private function recover(TopicPartition $tp, ErrorCode $error): void
    {
        $offset = $this->subscription->position($tp);

        if ($error === ErrorCode::OffsetOutOfRange) {
            if ($this->config->autoOffsetReset === Consumer\AutoOffsetReset::None) {
                throw new Exception\OffsetOutOfRange($tp, $offset);
            }

            // @infection-ignore-all
            $this->logger->warning('offset out of range, resetting', [
                'topic' => $tp->topic,
                'partition' => $tp->partition,
                'offset' => $offset,
                'reset' => $this->config->autoOffsetReset->name,
            ]);

            $this->cycle->reset($tp);

            return;
        }

        if (!$error->retriable()) {
            throw new Exception\ProtocolError(new Errors([new Error\PartitionError($tp->topic, $tp->partition, $error)]));
        }

        // @infection-ignore-all
        $this->logger->warning('partition fetch failed, retrying', [
            'topic' => $tp->topic,
            'partition' => $tp->partition,
            'offset' => $offset,
            'error' => $error->name,
        ]);

        $this->cycle->retry($tp);
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

        // The broker returns whole record batches, so a fetch that starts
        // mid-batch — e.g. resuming from a committed offset that falls inside a
        // batch — comes back with the records before it too. Drop anything below
        // the position we asked to start from, or we'd redeliver them.
        $position = $this->subscription->position($tp);

        $offset = null;

        foreach ($batches as $records) {
            // Even a control batch (no user records after filtering)
            // occupies offsets in the log, so the position must step past
            // it — otherwise the next fetch returns the same batch again.
            $offset = $records->nextOffset;

            if ($records->records === []) {
                continue; // control batch — nothing to hand to the consumer
            }

            $wanted = array_values(array_filter(
                $records->records,
                static fn(Consumer\Record $record) => $record->offset >= $position,
            ));

            if ($wanted === []) {
                continue; // the whole batch precedes the requested offset
            }

            // Backpressure: if the buffer is full this blocks until the
            // consumer drains it (or stop() cancels us).
            ($this->deliver)($wanted, $cancellation);
        }

        $this->subscription->advance($tp, $offset);
    }
}
