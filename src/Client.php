<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
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

    public function close(): void
    {
        $this->admin->close();
    }
}
