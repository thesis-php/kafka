<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Future;
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

    /**
     * @param \Traversable<array-key, ReadyBatch> $batches
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Producer\Config $config,
        private readonly \Traversable $batches,
    ) {
        $this->workers = new TopicPartition\Map();
        $this->loop = $this->run();
    }

    public function stop(): void
    {
        // Wait for the router loop to drain the accumulator and exit —
        // after this no new batches will be routed to workers
        $this->loop->await();

        // Now stop workers, nothing will enqueue into them anymore.
        $futures = [];
        foreach ($this->workers->values() as $worker) {
            $futures[] = async($worker->stop(...));
        }

        Future\awaitAll($futures);
    }

    /**
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
