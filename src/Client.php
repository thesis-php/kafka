<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final readonly class Client
{
    private Internal\Cluster $admin;

    private Internal\ContextLogger $logger;

    public function __construct(
        public Config $config,
        LoggerInterface $logger = new NullLogger(),
    ) {
        $logger = new Internal\ContextLogger($logger, [
            'component' => 'cluster',
            'clientName' => $config->clientSoftwareName,
            'clientVersion' => $config->clientSoftwareVersion,
        ]);

        $this->logger = $logger;
        $this->admin = new Internal\Cluster($config, $logger);
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws Exception
     * @throws CancelledException
     */
    public function request(Generated\Request $request, ?Cancellation $cancellation = null): Generated\Response
    {
        return $this->admin->request($request, $cancellation);
    }

    public function createProducer(Producer\Config $config = new Producer\Config()): Producer
    {
        return new Producer\Internal\AsyncProducer(
            cluster: new Internal\Cluster($this->config, $this->logger),
            config: $config,
            logger: $this->logger->with([
                'component' => 'producer',
                'idempotent' => $config->idempotent,
                'compressor' => $config->compressor->codec()->name,
                'inflight' => $config->maxInflightProduceRequest,
                'batchSize' => $config->batchSize,
                'linger' => $config->linger->toSeconds(3),
                'acks' => $config->acks->name,
            ]),
        );
    }

    /**
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     */
    public function createConsumer(
        string|array $topics,
        Consumer\Config $config = new Consumer\Config(),
    ): Consumer {
        if (!\is_array($topics)) {
            $topics = [$topics];
        }

        return new Consumer\Internal\AsyncConsumer(
            cluster: new Internal\Cluster($this->config, $this->logger),
            config: $config,
            requestTimeout: $this->config->requestTimeout,
            topics: $topics,
            logger: $this->logger->with([
                'component' => 'consumer',
                'topics' => implode(', ', $topics),
                'autoOffsetReset' => $config->autoOffsetReset->name,
                'isolationLevel' => $config->isolationLevel->name,
                'groupId' => $config->group?->groupId,
            ]),
        );
    }

    /**
     * Consume records in the background through a callback, the simplified
     * form most people reach for first.
     *
     * Creates a consumer for the given topics and config, then spawns a
     * background loop that drains record batches and invokes $handler for
     * each one. Each batch is a non-empty list of records from a single
     * (topic, partition), in offset order. The handler also receives a
     * {@see ConsumerSession}, so it can commit progress or stop the
     * subscription from inside.
     *
     * Returns immediately with a {@see ConsumerSubscription} handle: call
     * {@see ConsumerSubscription::close()} to stop the loop and release the
     * consumer, or {@see ConsumerSubscription::join()} to block until it
     * finishes (and surface any error the handler or fetch loop threw).
     *
     * For direct, pull-based control over reading — including Poll mode and
     * manual iteration — create a consumer with {@see createConsumer()} and
     * drive {@see Consumer::consume()} yourself.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param callable(non-empty-list<Consumer\Record>, ConsumerSession): void $handler
     */
    public function consume(
        string|array $topics,
        callable $handler,
        Consumer\Config $config = new Consumer\Config(),
    ): ConsumerSubscription {
        $consumer = $this->createConsumer($topics, $config);

        $ctx = new Consumer\Internal\CallableHandler($consumer);
        $ctx->handle($handler);

        return $ctx;
    }

    public function close(): void
    {
        $this->admin->close();
    }
}
