<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Sender;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Thesis\Kafka\Producer\Internal\ReadyBatch;
use function Amp\async;

/**
 * Sends the batches of one partition in the order of their release,
 * with at most the given number of requests in flight.
 *
 * @internal
 */
final class PartitionWorker
{
    /**
     * Batches that wait for a free in-flight slot, each with the future that
     * completes when a slot takes it.
     *
     * @var \SplQueue<array{ReadyBatch, DeferredFuture<null>}>
     */
    private readonly \SplQueue $waiting;

    private int $inflight = 0;

    /**
     * @param Cancellation $cancellation fails the batches that are not delivered yet
     * @param positive-int $maxInflightProduceRequests
     */
    public function __construct(
        private readonly BatchProducer $producer,
        private readonly Cancellation $cancellation,
        private readonly int $maxInflightProduceRequests,
    ) {
        $this->waiting = new \SplQueue();
    }

    /**
     * Never waits.
     *
     * @return Future<null> completes when an in-flight slot takes the batch
     */
    public function enqueue(ReadyBatch $batch): Future
    {
        /** @var DeferredFuture<null> $taken */
        $taken = new DeferredFuture();
        $this->waiting->enqueue([$batch, $taken]);
        $this->next();

        return $taken->getFuture();
    }

    private function next(): void
    {
        while ($this->inflight < $this->maxInflightProduceRequests && !$this->waiting->isEmpty()) {
            [$batch, $taken] = $this->waiting->dequeue();
            ++$this->inflight;
            $taken->complete();

            async($this->producer->produce(...), $batch, $this->cancellation)->finally(function (): void {
                --$this->inflight;
                $this->next();
            });
        }
    }
}
