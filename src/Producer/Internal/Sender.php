<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Future;
use Amp\Pipeline;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\TopicPartition;
use Thesis\Kafka\Producer;
use function Amp\async;

/**
 * @internal
 */
final class Sender
{
    /** @var Future<null> */
    private readonly Future $loop;

    /** @var TopicPartition\Map<Sender\PartitionWorker> */
    private TopicPartition\Map $workers;

    /** @var Pipeline\ConcurrentIterator<ReadyBatch> */
    private readonly Pipeline\ConcurrentIterator $batches;

    /**
     * @param Pipeline\Queue<ReadyBatch> $send
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Producer\Config $config,
        Pipeline\Queue $send,
    ) {
        $this->batches = $send->iterate();
        $this->workers = new TopicPartition\Map();
        $this->loop = $this->run();
    }

    public function stop(): void
    {
        // The queue must already be completed by the caller
        // so this loop sees its end and exits. Wait for that first:
        // once the router has drained and returned, no batch can still be
        // routed to a worker, which is what makes stopping workers next safe.
        $this->loop->await();

        // Workers can now be stopped — nothing will enqueue into them.
        $futures = [];
        foreach ($this->workers->values() as $worker) {
            $futures[] = async($worker->stop(...));
        }

        Future\awaitAll($futures);
    }

    /**
     * The router loop: pull each sealed batch off the shared queue and hand
     * it to the worker for its (topic, partition), creating that worker on
     * first sight. The loop itself does no I/O — it only routes; the workers
     * own the actual send, retry, and in-flight accounting, so a slow
     * partition stalls only its own worker, not the routing of others.
     *
     * @return Future<null>
     */
    private function run(): Future
    {
        $workers = &$this->workers;
        $cluster = $this->cluster;
        $config = $this->config;
        $batches = $this->batches;

        /** @var Future<null> */
        return async(static function () use (
            $cluster,
            $config,
            $batches,
            &$workers,
        ): void {
            $producer = new Sender\BatchProducer(
                $cluster,
                $config,
            );

            foreach ($batches as $batch) {
                $tp = new TopicPartition($batch->topic, $batch->partition);

                $worker = ($workers[$tp] ??= new Sender\PartitionWorker(
                    $producer,
                    $config->maxInflightProduceRequest,
                ));

                $worker->enqueue($batch);
            }
        });
    }
}
