<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Producer;
use function Amp\async;

/**
 * Sends records to Kafka, fanning them out to a {@see TopicProducer} per
 * topic (each owns the per-partition batching, partitioning, and linger
 * for its topic) and draining all of them through a single shared
 * {@see Sender}.
 *
 * Topic producers are created lazily on first produce to a topic. The
 * idempotent identity, when enabled, is obtained once and shared across
 * all topics — it belongs to the producer as a whole, not to any one
 * topic.
 *
 * @internal
 */
final class AsyncProducer implements Producer
{
    private readonly IdempotencyCoordinator $idempotencyCoordinator;

    /** @var array<non-empty-string, TopicProducer> */
    private array $producers = [];

    /**
     * The single queue every {@see TopicProducer} pushes sealed batches
     * into, and the single {@see Sender} that drains it.
     *
     * One queue and one sender across all topics — deliberately not one per
     * topic. Ordering only needs to hold per (topic, partition), and a
     * single sender routes each partition to its own worker, so per-topic
     * isolation would buy nothing. What it would cost is real: a partition
     * of topic A and a partition of topic B can share a leader, and one
     * sender can pack their batches into a single multi-topic ProduceRequest
     * to that broker. It also keeps the in-flight limit global rather than
     * multiplying it by the number of topics.
     *
     * @var Pipeline\Queue<ReadyBatch>
     */
    private readonly Pipeline\Queue $send;

    private readonly Sender $sender;

    /**
     * Ends the waits of produce() when close() is called.
     */
    private readonly DeferredCancellation $closing;

    /**
     * Fails the batches that are not delivered yet when a cancellation of close() fires.
     */
    private readonly DeferredCancellation $abort;

    /**
     * Null until close() is called. Completes when the producer is closed.
     *
     * @var ?Future<null>
     */
    private ?Future $closed = null;

    public function __construct(
        private readonly Cluster $cluster,
        private readonly Producer\Config $config,
        private readonly ContextLogger $logger,
    ) {
        $this->send = new Pipeline\Queue();
        $this->closing = new DeferredCancellation();
        $this->abort = new DeferredCancellation();
        $this->idempotencyCoordinator = new IdempotencyCoordinator(
            $cluster,
            // @infection-ignore-all
            $this->logger->with([
                'component' => 'producer.idempotency',
            ]),
            $this->closing->getCancellation(),
        );
        $this->sender = new Sender(
            cluster: $cluster,
            config: $config,
            send: $this->send,
            cancellation: $this->abort->getCancellation(),
            // @infection-ignore-all
            logger: $this->logger->with([
                'component' => 'producer.sender',
            ]),
        );
    }

    #[\Override]
    public function produce(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Future|array {
        if (!\is_array($records)) {
            return $this->doProduce($records, $cancellation);
        }

        $futures = [];

        foreach ($records as $record) {
            try {
                $futures[] = $this->doProduce($record, $cancellation);
            } catch (\Throwable $e) {
                if ($e instanceof Exception\ProducerClosed || ($e instanceof CancelledException && $cancellation->isRequested())) {
                    // The caller gets no futures, so nobody else can handle their errors.
                    foreach ($futures as $future) {
                        $future->ignore();
                    }

                    throw $e;
                }

                $futures[] = Future::error($e);
            }
        }

        return $futures;
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
        if ($this->closed !== null) {
            return;
        }

        $futures = [];

        foreach ($this->producers as $producer) {
            $futures[] = async($producer->flush(...));
        }

        Future\awaitAll($futures);
    }

    #[\Override]
    public function close(Cancellation $cancellation = new NullCancellation()): void
    {
        // The first call starts the shutdown. Every call waits for it.
        $this->closed ??= async($this->shutdown(...));

        $id = $cancellation->subscribe($this->abort->cancel(...));

        try {
            $this->closed->await();
        } finally {
            $cancellation->unsubscribe($id);
        }
    }

    private function shutdown(): void
    {
        // produce() is rejected from now on. Its waits end too.
        $this->closing->cancel();

        // @infection-ignore-all
        $this->logger->info('close producer', [
            'producers' => \count($this->producers),
        ]);

        // Each TopicProducer releases its open batches and waits until every
        // batch has a result. A cancellation of close() fails the rest.
        $futures = [];
        foreach ($this->producers as $producer) {
            $futures[] = async($producer->close(...));
        }

        Future\awaitAll($futures);

        // No batch is left, so nobody pushes into the queue any more.
        // The router loop of the Sender ends when the queue is completed.
        $this->send->complete();
        $this->sender->stop();
        $this->cluster->close();
    }

    /**
     * @return Future<Producer\Produced>
     * @throws Exception
     */
    private function doProduce(
        Producer\Record $record,
        Cancellation $cancellation,
    ): Future {
        $this->ensureOpen();

        $topic = $record->topic
            ?? $this->config->defaultTopic
            ?? throw new \LogicException('No producer record topic and no default');

        $idempotency = null;
        // @infection-ignore-all
        $logger = $this->logger->with([
            'topic' => $topic,
        ]);

        $wait = new CompositeCancellation($cancellation, $this->closing->getCancellation());

        try {
            if ($this->config->idempotent) {
                $idempotency = $this->idempotencyCoordinator->initialize($wait);
                // @infection-ignore-all
                $logger = $logger->with([
                    'producerId' => $idempotency->producerId,
                    'producerEpoch' => $idempotency->producerEpoch,
                ]);

                // The producer can close while it waits for the producer id.
                $this->ensureOpen();
            }

            $producer = $this->producers[$topic] ??= new TopicProducer(
                topic: $topic,
                partitioner: $this->config->partitioner->forTopic($topic),
                metadata: $this->cluster,
                idempotency: $this->idempotencyCoordinator,
                send: $this->send,
                config: $this->config,
                logger: $logger,
            );

            return $producer->queue($record, $idempotency, $wait);
        } catch (CancelledException $e) {
            $this->ensureOpen();

            throw $e;
        }
    }

    /**
     * @throws Exception\ProducerClosed
     */
    private function ensureOpen(): void
    {
        if ($this->closed !== null) {
            throw new Exception\ProducerClosed();
        }
    }
}
