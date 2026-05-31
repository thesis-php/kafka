<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Pipeline;
use Revolt\EventLoop;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Protocol\Record;
use Thesis\Kafka\Producer;

/**
 * @internal
 * @template-implements \IteratorAggregate<array-key, ReadyBatch>
 */
final class Accumulator implements \IteratorAggregate
{
    /**
     * Open batches, one per (topic, partition). Records append() into
     * these. Sealed batches are removed from here and pushed to $ready.
     *
     * @var array<non-empty-string, array<non-negative-int, OpenBatch>>
     */
    private array $open = [];

    /** @var array<non-empty-string, string> */
    private array $timers = [];

    /**
     * Sealed batches awaiting send.
     *
     * @var Pipeline\Queue<ReadyBatch>
     */
    private readonly Pipeline\Queue $ready;

    private Accumulator\State $state = Accumulator\State::Ready;

    /** @var non-empty-string */
    private string $batchId = 'a';

    /** @var list<\Closure(OpenBatch): void> */
    private array $callbacks = [];

    public function __construct(
        private readonly Producer\Config $config,
    ) {
        $this->ready = new Pipeline\Queue();
    }

    /**
     * Returns a future that completes when the batch
     * this record landed in has been sent and the broker responded.
     *
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @return Future<Producer\Produced>
     * @throws Exception
     */
    public function append(
        string $topic,
        int $partition,
        Producer\Record $record,
        ?Idempotency $idempotency = null,
        int $sequence = 0,
    ): Future {
        if ($this->state === Accumulator\State::Dead) {
            throw new \LogicException('Accumulator is dead, create the new one.');
        }

        $open = $this->open[$topic][$partition] ?? $this->openNew(
            topic: $topic,
            partition: $partition,
            idempotency: $idempotency,
            sequence: $sequence,
        );

        if ($this->batchReady($open, $record)) {
            if ($this->config->manualFlush) {
                throw new Exception\BatchFull();
            }

            $this->release($open);
            $open = null;
        }

        $open ??= $this->openNew(
            topic: $topic,
            partition: $partition,
            idempotency: $idempotency,
            sequence: $sequence,
        );

        /** @var DeferredFuture<Producer\Produced> $deferred */
        $deferred = new DeferredFuture();
        $open->batch->append($record);
        $open->futures[] = $deferred;

        if (!$this->config->manualFlush && $open->batch->estimatedSize() >= $this->config->batchSize) {
            $this->release($open);
        }

        return $deferred->getFuture();
    }

    /**
     * @param \Closure(OpenBatch): void $callback
     */
    public function onRelease(\Closure $callback): void
    {
        $this->callbacks[] = $callback;
    }

    public function flush(): void
    {
        /** @var list<Future<Producer\Produced>> $futures */
        $futures = [];

        foreach ($this->open as $partitions) {
            foreach ($partitions as $open) {
                $this->release($open);

                $futures = [
                    ...$futures,
                    ...array_map(
                        static fn(DeferredFuture $deferred) => $deferred->getFuture(),
                        $open->futures,
                    ),
                ];
            }
        }

        Future\awaitAll($futures);
    }

    public function close(): void
    {
        if ($this->state === Accumulator\State::Ready) {
            $this->state = Accumulator\State::Dead;
            $this->flush();
            $this->ready->complete();
        }
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        return $this->ready->iterate();
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     */
    private function openNew(
        string $topic,
        int $partition,
        ?Idempotency $idempotency = null,
        int $sequence = 0,
    ): OpenBatch {
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
            topic: $topic,
            partition: $partition,
            batch: $batch,
        );

        $this->open[$topic][$partition] = $open;

        if (!$this->config->manualFlush && $this->config->linger->isPositive()) {
            /** @var \WeakReference<OpenBatch> $reference */
            $reference = \WeakReference::create($open);

            $this->timers[$batchId] = EventLoop::delay(
                $this->config->linger->toSeconds(3),
                function () use ($reference): void {
                    $open = $reference->get();
                    if ($open !== null) {
                        $this->release($open);
                    }
                },
            );
        }

        return $open;
    }

    private function release(OpenBatch $open): void
    {
        unset($this->open[$open->topic][$open->partition]);

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

        // Run callbacks (partition rotation, sequence advance) BEFORE pushing to
        // the ready queue. push() suspends when the buffer is full, and during
        // that suspend a concurrent append for this (topic, partition) could open
        // a new batch — which must see the advanced sequence and rotated
        // partition. Running them first keeps that update synchronous and ordered
        // ahead of any work the suspend lets in.
        foreach ($this->callbacks as $callback) {
            $callback($open);
        }

        $this->ready->push($open->release());
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
