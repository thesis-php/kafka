<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\GroupBalancer;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Consumer\GroupMember;
use Thesis\Kafka\TopicPartition;

final readonly class RoundRobinGroupBalancerTest
{
    #[Test]
    public function dealsPartitionsInTurn(): void
    {
        $balancer = new RoundRobinGroupBalancer();

        $assigned = $balancer->assign(
            members: [
                new GroupMember('A', ['orders']),
                new GroupMember('B', ['orders']),
            ],
            partitionCounts: ['orders' => 6],
        );

        Assert::equals(
            $assigned['A'] ?? null,
            [
                new TopicPartition('orders', 0),
                new TopicPartition('orders', 2),
                new TopicPartition('orders', 4),
            ],
        );
        Assert::equals(
            $assigned['B'] ?? null,
            [
                new TopicPartition('orders', 1),
                new TopicPartition('orders', 3),
                new TopicPartition('orders', 5),
            ],
        );
    }

    #[Test]
    public function skipsMembersNotSubscribed(): void
    {
        $balancer = new RoundRobinGroupBalancer();

        $assigned = $balancer->assign(
            members: [
                new GroupMember('A', ['orders', 'payments']),
                new GroupMember('B', ['orders']),
            ],
            partitionCounts: [
                'orders' => 4,
                'payments' => 2,
            ],
        );

        Assert::equals(
            $assigned['A'] ?? null,
            [
                new TopicPartition('orders', 0),
                new TopicPartition('orders', 2),
                new TopicPartition('payments', 0),
                new TopicPartition('payments', 1),
            ],
        );
        Assert::equals(
            $assigned['B'] ?? null,
            [
                new TopicPartition('orders', 1),
                new TopicPartition('orders', 3),
            ],
        );
    }
}
