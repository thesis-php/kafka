<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\TopicPartition;
use Thesis\Kafka\Producer;
use function Amp\weakClosure;

/**
 * Sends records to Kafka. One {@see AsyncProducer} owns one {@see Accumulator} (per-
 * (topic, partition) batching) and one {@see Sender} (background coroutine
 * that drains the {@see Accumulator}).
 *
 * @internal
 */
final class AsyncProducer implements Producer
{
    private readonly IdempotencyCoordinator $idempotencyCoordinator;

    private readonly Accumulator $accumulator;

    private readonly Sender $sender;

    /** @var array<non-empty-string, Producer\TopicPartitioner> */
    private array $partitioners = [];

    public function __construct(
        private readonly Cluster $cluster,
        private readonly Producer\Config $config,
    ) {
        $this->idempotencyCoordinator = new IdempotencyCoordinator($cluster);
        $this->accumulator = new Accumulator($config);
        $this->sender = new Sender(
            cluster: $cluster,
            config: $config,
            batches: $this->accumulator,
        );

        $this->accumulator->onRelease(weakClosure($this->rotatePartitioner(...)));

        if ($config->idempotent) {
            $this->accumulator->onRelease(weakClosure($this->reserveSequence(...)));
        }
    }

    #[\Override]
    public function produce(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Future|array {
        if (\is_array($records)) {
            return array_map(
                fn(Producer\Record $record) => $this->doProduce($record, $cancellation),
                $records,
            );
        }

        return $this->doProduce($records, $cancellation);
    }

    #[\Override]
    public function produceSync(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Producer\Produced|array {
        $futures = $this->produce($records, $cancellation);
        if (\is_array($futures)) {
            /** @var non-empty-list<Producer\Produced> */
            return Future\await($futures);
        }

        return $futures->await();
    }

    #[\Override]
    public function flush(): void
    {
        $this->accumulator->flush();
    }

    #[\Override]
    public function close(): void
    {
        $this->accumulator->close();
        $this->sender->stop();
    }

    /**
     * @return Future<Producer\Produced>
     * @throws Exception
     */
    private function doProduce(
        Producer\Record $record,
        Cancellation $cancellation = new NullCancellation(),
    ): Future {
        $topic = $record->topic
            ?? $this->config->defaultTopic
            ?? throw new \LogicException('No producer record topic and no default');

        $topicMetadata = $this->cluster->metadata()->topics[$topic]
            ?? $this->cluster->refreshMetadata([$topic], $cancellation)->topics[$topic]
            ?? throw new UnknownTopic($topic);

        $partitioner = $this->partitioners[$topic] ??= $this->config->partitioner->forTopic($topic);

        $partition = $record->partition ?? $partitioner->partition($record, \count($topicMetadata)); // @phpstan-ignore argument.type

        $idempotency = null;
        $sequence = 0;
        if ($this->config->idempotent) {
            $idempotency = $this->idempotencyCoordinator->initialize($cancellation);
            $sequence = $this->idempotencyCoordinator->sequenceFor(new TopicPartition($topic, $partition));
        }

        return $this->accumulator->append(
            topic: $topic,
            partition: $partition,
            record: $record,
            idempotency: $idempotency,
            sequence: $sequence,
        );
    }

    private function reserveSequence(OpenBatch $batch): void
    {
        $count = \count($batch);
        \assert($count > 0);

        $tp = new TopicPartition($batch->topic, $batch->partition);
        $this->idempotencyCoordinator->reserveSequence($tp, $count);
    }

    private function rotatePartitioner(OpenBatch $batch): void
    {
        $partitioner = $this->partitioners[$batch->topic] ?? null;
        if ($partitioner instanceof Producer\StickyTopicPartitioner) {
            $metadata = $this->cluster->metadata()->topics[$batch->topic] ?? null;

            /** @var positive-int $partitions */
            $partitions = $metadata !== null ? \count($metadata->partitions) : 1;

            $partitioner->rotate(
                partition: $batch->partition,
                partitions: $partitions, // TODO: refactor metadata
            );
        }
    }
}
