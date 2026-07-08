<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final class DirectConsumer implements Consumer
{
    private readonly AssignmentConsumer $consumer;

    private readonly OffsetLister $offsets;

    /** @var ?Future<null> */
    private ?Future $setup = null;

    private DirectConsumer\State $state = DirectConsumer\State::Idle;

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Consumer\Config $config,
        private readonly TimeSpan $requestTimeout,
        private readonly array $topics,
        private readonly ContextLogger $logger,
    ) {
        $this->consumer = new AssignmentConsumer(
            cluster: $this->cluster,
            config: $this->config,
            logger: $this->logger,
        );
        $this->offsets = new OffsetLister(
            cluster: $this->cluster,
            config: $this->config,
            requestTimeout: $this->requestTimeout,
        );
    }

    #[\Override]
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->state === DirectConsumer\State::Closed) {
            throw new Exception\ConsumerClosed();
        }

        $this->setup?->await($cancellation);

        if ($this->state === DirectConsumer\State::Idle) {
            $this->state = DirectConsumer\State::Running;

            try {
                $this->setup ??= async($this->setup(...), $cancellation);
                $this->setup->await($cancellation);
            } catch (\Throwable $e) {
                $this->state = DirectConsumer\State::Idle;
                throw $e;
            } finally {
                $this->setup = null;
            }
        }

        return $this->consumer->consume($mode, $cancellation);
    }

    #[\Override]
    public function commit(
        Consumer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): void {
        throw new Exception\NoGroupConfigured(
            'Commit requires a consumer group, this consumer has none configured.',
        );
    }

    #[\Override]
    public function close(): void
    {
        if ($this->state === DirectConsumer\State::Closed) {
            return;
        }

        $this->state = DirectConsumer\State::Closed;
        $this->consumer->stop();
        $this->cluster->close();
    }

    /**
     * @throws Exception
     */
    private function setup(?Cancellation $cancellation = null): void
    {
        $cancellation ??= new NullCancellation();

        $metadata = $this->cluster->refreshMetadata($this->topics, $cancellation);

        /** @var list<TopicPartition> $partitions */
        $partitions = [];
        foreach ($this->topics as $topic) {
            $topicMetadata = $metadata->topics[$topic]
                ?? throw new Exception\UnknownTopic($topic);

            /** @var non-negative-int $partition */
            foreach ($topicMetadata->partitions as $partition => $_) {
                $partitions[] = new TopicPartition($topic, $partition);
            }
        }

        \assert($partitions !== []);

        $offsets = $this->offsets->list($partitions, $metadata, $cancellation);
        $this->consumer->start($offsets, $cancellation);

        $this->logger->debug('consumer started', [
            'topics' => $this->topics,
        ]);
    }
}
