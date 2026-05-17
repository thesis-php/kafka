<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Config\ProducerConfig;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Internal\Producer\Accumulator;
use Thesis\Kafka\Internal\Producer\Sender;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\ProduceRecord;
use Thesis\Kafka\ProduceResult;
use Thesis\Kafka\StickyTopicPartitioner;
use Thesis\Kafka\TopicPartitioner;
use function Amp\weakClosure;

/**
 * Sends records to Kafka. One {@see Producer} owns one {@see Accumulator} (per-
 * (topic, partition) batching) and one {@see Sender} (background coroutine
 * that drains the {@see Accumulator}).
 *
 * @internal
 */
final class Producer
{
    private readonly Accumulator $accumulator;

    private readonly Sender $sender;

    /** @var array<non-empty-string, TopicPartitioner> */
    private array $partitioners = [];

    public function __construct(
        private readonly Cluster $cluster,
        private readonly ProducerConfig $config,
    ) {
        $this->accumulator = new Accumulator(
            compressor: $config->compressor,
            crc32: $config->crc32,
            batchSize: $config->batchSize,
            linger: $config->linger,
        );

        $this->sender = new Sender(
            cluster: $cluster,
            config: $config,
            batches: $this->accumulator,
        );

        $this->accumulator->onRelease(weakClosure($this->rotatePartitioner(...)));
        $this->sender->start();
    }

    /**
     * Produce a record asynchronously. The returned future completes
     * when the broker acknowledges the write (or with an error if the write failed).
     *
     * For {@see Acks::None} the future completes as soon as the
     * write hits the socket, with offset and timestamp set to null —
     * the broker doesn't send a response, so neither is known.
     *
     * @return Future<ProduceResult>
     * @throws KafkaException
     */
    public function produce(ProduceRecord $record, Cancellation $cancellation = new NullCancellation()): Future
    {
        $topic = $record->topic
            ?? $this->config->defaultTopic
            ?? throw new \LogicException('No producer record topic and no default');

        $topicMetadata = $this->cluster->metadata()->topics[$topic]
            ?? $this->cluster->refreshMetadata([$topic], $cancellation)->topics[$topic]
            ?? throw new UnknownTopic($topic);

        $partitioner = $this->partitioners[$topic] ??= $this->config->partitioner->forTopic($topic);

        $partition = $record->partition ?? $partitioner->partition($record, \count($topicMetadata)); // @phpstan-ignore argument.type

        return $this->accumulator->append($topic, $partition, $record);
    }

    public function close(): void
    {
        $this->accumulator->close();
        $this->sender->stop();
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     */
    private function rotatePartitioner(string $topic, int $partition): void
    {
        $partitioner = $this->partitioners[$topic] ?? null;
        if ($partitioner instanceof StickyTopicPartitioner) {
            $metadata = $this->cluster->metadata()->topics[$topic] ?? null;

            /** @var positive-int $partitions */
            $partitions = $metadata !== null ? \count($metadata->partitions) : 1;

            $partitioner->rotate(
                partition: $partition,
                partitions: $partitions, // TODO: refactor metadata
            );
        }
    }
}
