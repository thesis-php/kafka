<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
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
     * these. Sealed batches are removed from here and pushed to $send.
     *
     * @var array<non-negative-int, OpenBatch>
     */
    private array $open = [];

    /** @var array<non-empty-string, string> */
    private array $timers = [];

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
     * @param Pipeline\Queue<ReadyBatch> $send
     */
    public function __construct(
        private readonly string $topic,
        private readonly Producer\TopicPartitioner $partitioner,
        private readonly MetadataProvider $metadata,
        private readonly IdempotencyCoordinator $idempotency,
        private readonly Pipeline\Queue $send,
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

        // The producer can close while it waits for the metadata.
        $this->ensureReady();

        $partition = $record->partition ?? $this->partitioner->partition($record, $topicMetadata->partitions());

        $open = $this->open[$partition] ?? $this->openNew(
            partition: $partition,
            idempotency: $idempotency,
        );

        if ($this->batchReady($open, $record)) {
            if ($this->config->manualFlush) {
                throw new Exception\BatchFull();
            }

            $this->release($open, ContextLogger\ReleaseReasonContextKey::Size);
            $open = null;

            // The producer can close while the full batch waits for the queue.
            $this->ensureReady();
        }

        $open ??= $this->openNew(
            partition: $partition,
            idempotency: $idempotency,
        );

        /** @var DeferredFuture<Producer\Produced> $deferred */
        $deferred = new DeferredFuture();
        $open->batch->append($record);
        $open->futures[] = $deferred;

        if (!$this->config->manualFlush && $open->batch->estimatedSize() >= $this->config->batchSize) {
            $this->release($open, ContextLogger\ReleaseReasonContextKey::Size);
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
     */
    private function openNew(
        int $partition,
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

    private function release(OpenBatch $open, ContextLogger\ReleaseReasonContextKey $reason): void
    {
        unset($this->open[$open->partition]);

        if ($open->state === OpenBatch\State::Released) {
            return;
        }

        $timerId = $this->timers[$open->id] ?? null;
        if ($timerId !== null) {
            unset($this->timers[$open->id]);
            EventLoop::cancel($timerId);
        }

        if (\count($open) === 0) {
            // Sealed without any appends. Can happen if linger
            // fires but no records actually accumulated.
            return;
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

        // Rotate the sticky partitioner and advance the idempotent sequence
        // BEFORE pushing to the send queue. push() can suspend when the
        // queue is full, and during that suspend a concurrent queue() for
        // this partition could open a new batch — which must see the rotated
        // partition and the advanced sequence. Doing both first keeps that
        // state update synchronous, ahead of anything the suspend lets in.
        if ($this->partitioner instanceof Producer\StickyTopicPartitioner) {
            $topicMetadata = $this->metadata->topic($this->topic);

            $this->partitioner->rotate(
                partition: $open->partition,
                partitions: $topicMetadata->partitions(),
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

        // Register the batch before push(): a flush() during the push must wait for it.
        $this->released[$id] = $ready->done();
        $ready->done()->finally(function () use ($id): void {
            unset($this->released[$id]);
        });

        $this->send->push($ready);
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
