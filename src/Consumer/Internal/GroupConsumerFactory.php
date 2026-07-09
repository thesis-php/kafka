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
final readonly class GroupConsumerFactory
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
        Consumer\GroupConfig $group,
    ): GroupConsumer {
        $cluster = new Cluster(
            $this->config,
            $this->logger,
        );

        $logger = $this->logger->with([
            'component' => 'group-consumer',
            'topics' => implode(', ', $topics),
            'autoOffsetReset' => $config->autoOffsetReset->name,
            'isolationLevel' => $config->isolationLevel->name,
            'groupId' => $group->groupId,
            'groupInstanceId' => $group->groupInstanceId,
            'autocommit' => $group->autocommit,
            'balancers' => array_map(
                static fn(Consumer\GroupBalancer $balancer) => $balancer->name(),
                $group->balancers,
            ),
        ]);

        $coordinator = new GroupConsumer\Coordinator(
            groupId: $group->groupId,
            cluster: $cluster,
            logger: $logger,
        );

        $consumer = new AssignmentConsumer(
            cluster: $cluster,
            config: $config,
            logger: $logger,
        );

        $joinController = new GroupConsumer\JoinController(
            cluster: $cluster,
            coordinator: $coordinator,
            group: $group,
            topics: $topics,
        );

        $offsets = new GroupConsumer\OffsetResolver(
            cluster: $cluster,
            coordinator: $coordinator,
            lister: new OffsetLister(
                cluster: $cluster,
                config: $config,
                requestTimeout: $this->config->requestTimeout,
            ),
            groupId: $group->groupId,
            autoOffsetReset: $config->autoOffsetReset,
        );

        $committer = new GroupConsumer\OffsetCommitter(
            cluster: $cluster,
            coordinator: $coordinator,
            group: $group,
        );

        return new GroupConsumer(
            cluster: $cluster,
            logger: $logger,
            consumer: $consumer,
            config: $config,
            group: $group,
            coordinator: $coordinator,
            joinController: $joinController,
            offsets: $offsets,
            committer: $committer,
        );
    }
}
