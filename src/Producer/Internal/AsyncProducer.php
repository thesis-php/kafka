<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
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

    private bool $closed = false;

    public function __construct(
        private readonly Cluster $cluster,
        private readonly Producer\Config $config,
        private readonly ContextLogger $logger,
    ) {
        $this->send = new Pipeline\Queue();
        $this->idempotencyCoordinator = new IdempotencyCoordinator(
            $cluster,
            // @infection-ignore-all
            $this->logger->with([
                'component' => 'producer.idempotency',
            ]),
        );
        $this->sender = new Sender(
            cluster: $cluster,
            config: $config,
            send: $this->send,
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
        $futures = [];

        foreach ($this->producers as $producer) {
            $futures[] = async($producer->flush(...));
        }

        Future\awaitAll($futures);
    }

    #[\Override]
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        // @infection-ignore-all
        $this->logger->info('close producer', [
            'producers' => \count($this->producers),
        ]);

        // Shut down in dependency order: writers first, then the queue they
        // write into, then the reader of that queue.

        // 1. Close every TopicProducer. Each flushes its open batches into
        //    the queue and stops accepting writes — pushing after the queue
        //    is completed would be an error, so all writers must be done
        //    before we complete it.
        $futures = [];
        foreach ($this->producers as $producer) {
            $futures[] = async($producer->close(...));
        }

        Future\awaitAll($futures);

        // 2. Complete the queue. With no writers left, this is the signal the
        //    Sender's router loop watches for — its foreach over the queue
        //    ends once the buffer drains.
        $this->send->complete();

        // 3. Stop the Sender. It waits for the router loop to finish draining
        //    and for every partition worker to flush its in-flight batches,
        //    so close() returns only once all records have actually been sent.
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

        $idempotency = null;
        // @infection-ignore-all
        $logger = $this->logger->with([
            'topic' => $topic,
        ]);

        if ($this->config->idempotent) {
            $idempotency = $this->idempotencyCoordinator->initialize($cancellation);
            // @infection-ignore-all
            $logger = $logger->with([
                'producerId' => $idempotency->producerId,
                'producerEpoch' => $idempotency->producerEpoch,
            ]);
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

        return $producer->queue($record, $idempotency);
    }
}
