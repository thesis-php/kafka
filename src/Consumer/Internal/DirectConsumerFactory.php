<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Thesis\Kafka\Config;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;

/**
 * @internal
 */
final readonly class DirectConsumerFactory
{
    public function __construct(
        private Config $config,
        private ContextLogger $logger,
    ) {}

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function create(
        array $topics,
        Consumer\Config $config,
    ): DirectConsumer {
        $cluster = new Cluster(
            $this->config,
            $this->logger,
        );

        $logger = $this->logger->with([
            'component' => 'direct-consumer',
            'topics' => implode(', ', $topics),
            'autoOffsetReset' => $config->autoOffsetReset->name,
            'isolationLevel' => $config->isolationLevel->name,
        ]);

        $consumer = new AssignmentConsumer(
            cluster: $cluster,
            config: $config,
            logger: $logger,
        );

        $offsets = new OffsetLister(
            cluster: $cluster,
            config: $config,
            requestTimeout: $this->config->requestTimeout,
        );

        return new DirectConsumer(
            cluster: $cluster,
            consumer: $consumer,
            offsets: $offsets,
            topics: $topics,
            logger: $logger,
        );
    }
}
