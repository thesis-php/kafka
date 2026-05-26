<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Partitioner;

use Thesis\Kafka\Producer\Hasher;
use Thesis\Kafka\Producer\Partitioner;
use Thesis\Kafka\Producer\TopicPartitioner;

/**
 * @api
 */
final readonly class HashPartitioner implements Partitioner
{
    public function __construct(
        private Hasher $hasher = new Hasher\Murmur2Hasher(),
        private Partitioner $whenKeyIsNull = new Partitioner\RoundRobinPartitioner(),
    ) {}

    #[\Override]
    public function forTopic(string $topic): TopicPartitioner
    {
        return new HashTopicPartitioner(
            $this->hasher,
            $this->whenKeyIsNull->forTopic($topic),
        );
    }
}
