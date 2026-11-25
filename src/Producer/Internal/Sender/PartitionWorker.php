<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Sender;

use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Pipeline;
use Amp\Sync\LocalSemaphore;
use Thesis\Kafka\Producer\Internal\ReadyBatch;
use function Amp\async;

/**
 * @internal
 */
final class PartitionWorker
{
    /** @var Pipeline\Queue<ReadyBatch> */
    private readonly Pipeline\Queue $queue;

    private PartitionWorker\State $state = PartitionWorker\State::Running;

    private readonly DeferredCancellation $cancel;

    /** @var Future<null> */
    private readonly Future $loop;

    /** @var array<non-empty-string, Future<null>> */
    private array $writes = [];

    /** @var non-empty-string */
    private string $writeId = 'a';

    /**
     * @param positive-int $maxInflightProduceRequests
     */
    public function __construct(
        private readonly BatchProducer $producer,
        private readonly int $maxInflightProduceRequests = 1,
    ) {
        $this->queue = new Pipeline\Queue();
        $this->cancel = new DeferredCancellation();
        $this->loop = $this->run();
    }

    public function enqueue(ReadyBatch $batch): void
    {
        // The sender stops workers only after its router loop ends.
        // So a stopped worker gets no batches.
        $this->queue->push($batch);
    }

    public function stop(): void
    {
        if ($this->state === PartitionWorker\State::Running) {
            $this->state = PartitionWorker\State::Closed;
            $this->queue->complete();
            $this->cancel->cancel();
            $this->loop->await();
        }
    }

    /**
     * @return Future<null>
     */
    private function run(): Future
    {
        $producer = $this->producer;
        $queue = $this->queue;
        $cancellation = $this->cancel->getCancellation();
        $maxInflight = $this->maxInflightProduceRequests;
        $writes = &$this->writes;
        $writeId = &$this->writeId;

        /** @var Future<null> */
        return async(static function () use (
            $producer,
            $queue,
            $cancellation,
            $maxInflight,
            &$writes,
            &$writeId,
        ): void {
            $semaphore = new LocalSemaphore($maxInflight);

            $batches = $queue->iterate();

            foreach ($batches as $batch) {
                $lock = $semaphore->acquire();
                $idx = $writeId;
                $writeId = str_increment($writeId);
                $writes[$idx] = async($producer->produce(...), $batch, $cancellation)->finally(static function () use (
                    $lock,
                    $idx,
                    &$writes,
                ): void {
                    $lock->release();
                    unset($writes[$idx]);
                });
            }
        });
    }
}
