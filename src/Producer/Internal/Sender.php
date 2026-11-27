<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\Future;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Producer;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;

/**
 * Gives each released batch to the worker of its (topic, partition). The
 * worker is created on first sight. Each worker sends its own batches, so a
 * slow partition holds back only its own batches. The hand-over never waits.
 *
 * @internal
 */
final readonly class Sender
{
    /** @var TopicPartition\Map<Sender\PartitionWorker> */
    private TopicPartition\Map $workers;

    private Sender\BatchProducer $producer;

    /**
     * @param TimeSpan $requestTimeout the time for the network that each Produce attempt gets
     *                                 in addition to {@see Producer\Config::$produceTimeout}
     * @param Cancellation $cancellation fails the batches that are not delivered yet
     */
    public function __construct(
        Cluster $cluster,
        private Producer\Config $config,
        TimeSpan $requestTimeout,
        private Cancellation $cancellation,
        ContextLogger $logger,
    ) {
        $this->workers = new TopicPartition\Map();
        $this->producer = new Sender\BatchProducer(
            $cluster,
            $config,
            $requestTimeout,
            // @infection-ignore-all
            $logger->with([
                'component' => 'producer.batchProducer',
            ]),
        );
    }

    /**
     * The batches of one partition are sent in the order of the calls.
     *
     * @return Future<null> completes when the partition takes the batch to send it
     */
    public function send(ReadyBatch $batch): Future
    {
        $tp = new TopicPartition($batch->topic, $batch->partition);

        $worker = ($this->workers[$tp] ??= new Sender\PartitionWorker(
            $this->producer,
            $this->cancellation,
            $this->config->maxInflightProduceRequest,
        ));

        return $worker->enqueue($batch);
    }
}
