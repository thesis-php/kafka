<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\Future;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\RequestTimeout;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final class DirectConsumer implements Consumer
{
    /** @var ?Future<null> */
    private ?Future $setup = null;

    private DirectConsumer\State $state = DirectConsumer\State::Idle;

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly AssignmentConsumer $consumer,
        private readonly OffsetLister $offsets,
        private readonly array $topics,
        private readonly ContextLogger $logger,
        private readonly TimeSpan $requestTimeout,
    ) {}

    #[\Override]
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->state === DirectConsumer\State::Closed) {
            throw new Exception\ConsumerClosed();
        }

        if ($this->state === DirectConsumer\State::Idle) {
            $this->state = DirectConsumer\State::Running;

            // The setup does not use the cancellation of this caller: another
            // caller can wait for it too. Each caller waits with its own cancellation.
            $this->setup = async($this->setup(...))->ignore();
        }

        $this->setup?->await($cancellation);

        return $this->consumer->consume($mode, $cancellation);
    }

    #[\Override]
    public function commitRecords(Consumer\Record|array $records): void {}

    #[\Override]
    public function commitUncommitted(): void {}

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
    private function setup(): void
    {
        try {
            $this->start();
        } catch (\Throwable $e) {
            $this->state = DirectConsumer\State::Idle;

            throw $e;
        } finally {
            $this->setup = null;
        }
    }

    /**
     * @throws Exception
     */
    private function start(): void
    {
        $metadata = $this->cluster->lookup($this->topics, $this->timeout());

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

        $offsets = $this->offsets->list($partitions, $metadata, $this->timeout());
        $this->consumer->start($offsets, $this->timeout());

        // @infection-ignore-all
        $this->logger->debug('consumer started', [
            'topics' => $this->topics,
        ]);
    }

    /**
     * No caller limits the setup, so each step has the request timeout of the
     * client. Without it a broker that does not answer blocks the setup forever.
     */
    private function timeout(): Cancellation
    {
        return RequestTimeout::start($this->requestTimeout->toSeconds(3));
    }
}
