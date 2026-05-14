<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Thesis\Kafka\Partitioner;
use Thesis\Kafka\TopicPartitioner;

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
