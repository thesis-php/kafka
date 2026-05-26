<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Partitioner;

use Thesis\Kafka\Producer\Partitioner;
use Thesis\Kafka\Producer\TopicPartitioner;

/**
 * @api
 */
final readonly class RoundRobinPartitioner implements Partitioner
{
    #[\Override]
    public function forTopic(string $topic): TopicPartitioner
    {
        return new RoundRobinTopicPartitioner();
    }
}
