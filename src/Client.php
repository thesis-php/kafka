<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\CancelledException;
use Thesis\Kafka\Protocol\Generated;

/**
 * @api
 */
final readonly class Client
{
    private Internal\Cluster $admin;

    public function __construct(
        public Config $config,
    ) {
        $this->admin = new Internal\Cluster($config);
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
            new Internal\Cluster($this->config),
            $config,
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
            cluster: new Internal\Cluster($this->config),
            config: $config,
            requestTimeout: $this->config->requestTimeout,
            topics: $topics,
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
