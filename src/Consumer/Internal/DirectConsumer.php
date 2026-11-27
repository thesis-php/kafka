<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
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
 * Not started, starting, reading, or closed. A failed setup leaves the consumer
 * not started, so the next {@see consume()} runs the setup again. A fatal error
 * of the fetch closes the consumer.
 *
 * @internal
 */
final class DirectConsumer implements Consumer
{
    /** @var ?Future<null> */
    private ?Future $setup = null;

    /** @var ?Future<null> */
    private ?Future $shutdown = null;

    private DirectConsumer\State $state = DirectConsumer\State::Idle;

    /**
     * Fires at close(). It ends the setup and the callers that wait for it.
     */
    private readonly DeferredCancellation $closing;

    /**
     * @param non-empty-list<non-empty-string> $topics
     * @param Buffer<non-empty-list<Consumer\Record>> $buffer
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly AssignmentConsumer $consumer,
        private readonly OffsetLister $offsets,
        private readonly array $topics,
        private readonly ContextLogger $logger,
        private readonly TimeSpan $requestTimeout,
        private readonly Buffer $buffer,
    ) {
        $this->closing = new DeferredCancellation();
    }

    #[\Override]
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->state === DirectConsumer\State::Idle) {
            $this->state = DirectConsumer\State::Starting;

            // The setup does not use the cancellation of this caller: another
            // caller can wait for it too. Each caller waits with its own cancellation.
            $this->setup = async($this->setup(...))->ignore();
        }

        $setup = $this->setup;

        if ($setup !== null) {
            try {
                $closing = $this->closing->getCancellation();
                $setup->await($cancellation === null ? $closing : new CompositeCancellation($cancellation, $closing));
            } catch (\Throwable $e) {
                if ($this->state !== DirectConsumer\State::Closed) {
                    throw $e;
                }
            }

            // close() during the setup ends the callers that wait for it.
            if ($this->state === DirectConsumer\State::Closed) {
                return [];
            }
        }

        return $this->buffer->read($mode, $cancellation, self::records(...));
    }

    #[\Override]
    public function commitRecords(Consumer\Record|array $records): void {}

    #[\Override]
    public function commitUncommitted(): void {}

    #[\Override]
    public function close(): void
    {
        if ($this->state !== DirectConsumer\State::Closed) {
            $this->state = DirectConsumer\State::Closed;
            $this->closing->cancel();
            $this->buffer->close();
            $this->shutdown = async($this->shutdown(...));
        }

        $this->shutdown?->await();
    }

    /**
     * @throws Exception
     */
    private function setup(): void
    {
        try {
            $this->start();
        } catch (\Throwable $e) {
            // A failed setup is not fatal. A close() during the setup wins.
            if ($this->state === DirectConsumer\State::Starting) {
                $this->state = DirectConsumer\State::Idle;
            }

            throw $e;
        } finally {
            $this->setup = null;
        }

        // close() came during the setup, before the fetch started. So it did not stop the fetch.
        if ($this->state === DirectConsumer\State::Closed) {
            $this->consumer->stop();

            return;
        }

        $this->state = DirectConsumer\State::Running;
    }

    /**
     * The fetch calls it from one of its loops, so the consumer stops in another
     * coroutine: the stop of the fetch waits for every loop.
     */
    private function fail(\Throwable $error): void
    {
        if ($this->state !== DirectConsumer\State::Running) {
            return;
        }

        $this->state = DirectConsumer\State::Closed;
        $this->buffer->fail($error);
        $this->shutdown = async($this->shutdown(...));
    }

    private function shutdown(): void
    {
        $this->consumer->stop();
        $this->cluster->close();
    }

    /**
     * @param non-empty-list<Consumer\Record> $records
     * @return non-empty-list<Consumer\Record>
     */
    private static function records(array $records): array
    {
        return $records;
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
        $this->consumer->start(
            $offsets,
            deliver: $this->buffer->push(...),
            failed: $this->fail(...),
            cancellation: $this->timeout(),
        );

        // @infection-ignore-all
        $this->logger->debug('consumer started', [
            'topics' => $this->topics,
        ]);
    }

    /**
     * No caller limits the setup, so each step has the request timeout of the
     * client. Without it a broker that does not answer blocks the setup forever.
     * close() ends each step at once.
     */
    private function timeout(): Cancellation
    {
        return RequestTimeout::start($this->requestTimeout->toSeconds(3), $this->closing->getCancellation());
    }
}
