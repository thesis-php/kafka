<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Revolt\EventLoop;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\MetadataProvider;
use Thesis\Kafka\Internal\Protocol\Record;
use Thesis\Kafka\Producer;
use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final class TopicProducer
{
    /**
     * Open batches, one per partition. Records append() into
     * these. Sealed batches are removed from here and given to the sender.
     *
     * @var array<non-negative-int, OpenBatch>
     */
    private array $open = [];

    /** @var array<non-empty-string, string> */
    private array $timers = [];

    /**
     * The last batch of each partition that a call released by size. It
     * completes when the partition takes the batch. Until then, the calls for
     * this partition wait.
     *
     * @var array<non-negative-int, Future<null>>
     */
    private array $taken = [];

    /**
     * Released batches that are not delivered yet, by batch id.
     *
     * @var array<non-empty-string, Future<null>>
     */
    private array $released = [];

    private TopicProducer\State $state = TopicProducer\State::Ready;

    /** @var non-empty-string */
    private string $batchId = 'a';

    /**
     * @param non-empty-string $topic
     */
    public function __construct(
        private readonly string $topic,
        private readonly Producer\TopicPartitioner $partitioner,
        private readonly MetadataProvider $metadata,
        private readonly IdempotencyCoordinator $idempotency,
        private readonly Sender $sender,
        private readonly Producer\Config $config,
        private readonly ContextLogger $logger,
    ) {}

    /**
     * Returns a future that completes when the batch
     * this record landed in has been sent and the broker responded.
     *
     * @return Future<Producer\Produced>
     * @throws Exception
     */
    public function queue(
        Producer\Record $record,
        ?Idempotency $idempotency = null,
        Cancellation $cancellation = new NullCancellation(),
    ): Future {
        $this->ensureReady();

        $topicMetadata = $this->metadata->topic($this->topic, $cancellation);
        $partitions = $topicMetadata->partitions();
        $partition = $record->partition ?? $this->partitioner->partition($record, $partitions);

        while (true) {
            // The producer can close while this call waits.
            $this->ensureReady();

            // A slow partition makes only the callers of this partition wait.
            // A full batch that a call released is in the sender already, also when this wait is cancelled.
            $taken = $this->taken[$partition] ?? null;
            if ($taken !== null && !$taken->isComplete()) {
                $taken->await($cancellation);

                continue;
            }

            // Another call can open a batch for this partition while this call waits.
            // So the call always takes the batch that is open now.
            $open = $this->open[$partition] ?? $this->openNew(
                partition: $partition,
                partitions: $partitions,
                idempotency: $idempotency,
            );

            if (!$this->batchReady($open, $record)) {
                break;
            }

            if ($this->config->manualFlush) {
                throw new Exception\BatchFull();
            }

            $this->taken[$partition] = $this->release($open, ContextLogger\ReleaseReasonContextKey::Size);
        }

        /** @var DeferredFuture<Producer\Produced> $deferred */
        $deferred = new DeferredFuture();
        $open->batch->append($record);
        $open->futures[] = $deferred;

        if (!$this->config->manualFlush && $open->batch->estimatedSize() >= $this->config->batchSize) {
            // The record is in the batch already, so this call does not wait. The next call does.
            $this->taken[$partition] = $this->release($open, ContextLogger\ReleaseReasonContextKey::Size);
        }

        return $deferred->getFuture();
    }

    /**
     * Releases the open batches and waits until every batch released
     * before has a result. The errors of records stay in their futures.
     */
    public function flush(): void
    {
        foreach ($this->open as $batch) {
            $this->release($batch, ContextLogger\ReleaseReasonContextKey::Manual);
        }

        Future\awaitAll($this->released);
    }

    /**
     * Stops new records, then flushes.
     */
    public function close(): void
    {
        $this->state = TopicProducer\State::Dead;
        $this->flush();
    }

    /**
     * @param non-negative-int $partition
     * @param positive-int $partitions
     */
    private function openNew(
        int $partition,
        int $partitions,
        ?Idempotency $idempotency = null,
    ): OpenBatch {
        $sequence = 0;
        if ($idempotency !== null) {
            $sequence = $this->idempotency->sequenceFor(
                new TopicPartition($this->topic, $partition),
            );
        }

        $batch = new Protocol\RecordBatch(
            compressor: $this->config->compressor,
            attributes: new Record\Attributes(
                compression: $this->config->compressor->codec(),
            ),
            crc32c: $this->config->crc32,
            producerId: $idempotency->producerId ?? -1,
            producerEpoch: $idempotency->producerEpoch ?? -1,
            baseSequence: $sequence,
        );

        $batchId = $this->batchId;
        $this->batchId = str_increment($this->batchId);

        $open = new OpenBatch(
            id: $batchId,
            topic: $this->topic,
            partition: $partition,
            partitions: $partitions,
            batch: $batch,
        );

        $this->open[$partition] = $open;

        // @infection-ignore-all
        $this->logger->debug('open batch', [
            'partition' => $partition,
            'batchId' => $batchId,
            'baseSequence' => $sequence,
        ]);

        if (!$this->config->manualFlush && $this->config->linger->isPositive()) {
            /** @var \WeakReference<OpenBatch> $reference */
            $reference = \WeakReference::create($open);

            $this->timers[$batchId] = EventLoop::delay(
                $this->config->linger->toSeconds(3),
                function () use ($reference): void {
                    $open = $reference->get();
                    if ($open !== null) {
                        $this->release($open, ContextLogger\ReleaseReasonContextKey::Linger);
                    }
                },
            );
        }

        return $open;
    }

    /**
     * Never waits, so a concurrent call sees the result of the whole release.
     *
     * @return Future<null> completes when the partition takes the batch to send it
     */
    private function release(OpenBatch $open, ContextLogger\ReleaseReasonContextKey $reason): Future
    {
        unset($this->open[$open->partition]);

        if ($open->state === OpenBatch\State::Released) {
            return Future::complete();
        }

        $timerId = $this->timers[$open->id] ?? null;
        if ($timerId !== null) {
            unset($this->timers[$open->id]);
            EventLoop::cancel($timerId);
        }

        if (\count($open) === 0) {
            // Sealed without any appends. Can happen if linger
            // fires but no records actually accumulated.
            return Future::complete();
        }

        // @infection-ignore-all
        $this->logger->debug('release batch', [
            'partition' => $open->partition,
            'batchId' => $open->id,
            'recordCount' => \count($open),
            'estimatedSize' => $open->batch->estimatedSize(),
            'reason' => $reason->name,
            'baseSequence' => $open->batch->baseSequence,
        ]);

        // A lookup of the metadata could wait or throw, also in the linger timer.
        // So the partition count is the one that the batch got when it was opened.
        if ($this->partitioner instanceof Producer\StickyTopicPartitioner) {
            $this->partitioner->rotate(
                partition: $open->partition,
                partitions: $open->partitions,
            );
        }

        if ($this->config->idempotent) {
            $this->idempotency->reserveSequence(
                new TopicPartition($this->topic, $open->partition),
                \count($open),
            );
        }

        $ready = $open->release();
        $id = $open->id;

        $this->released[$id] = $ready->done();
        $ready->done()->finally(function () use ($id): void {
            unset($this->released[$id]);
        });

        // The order of the sequences is the order of the batches in the sender.
        return $this->sender->send($ready);
    }

    /**
     * @throws Exception\ProducerClosed
     */
    private function ensureReady(): void
    {
        if ($this->state === TopicProducer\State::Dead) {
            throw new Exception\ProducerClosed();
        }
    }

    private function batchReady(OpenBatch $open, Producer\Record $record): bool
    {
        if (\count($open) === 0) {
            return false;
        }

        $expectedSize = $open->batch->estimatedSize() + Protocol\RecordBatch::recordOverhead($record);

        return $expectedSize >= $this->config->batchSize;
    }
}
