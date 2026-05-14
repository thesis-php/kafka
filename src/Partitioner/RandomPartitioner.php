<?php

declare(strict_types=1);

namespace Thesis\Kafka\Partitioner;

use Random\Randomizer;
use Thesis\Kafka\Partitioner;
use Thesis\Kafka\TopicPartitioner;

/**
 * @api
 */
final readonly class RandomPartitioner implements Partitioner
{
    public function __construct(
        private Randomizer $randomizer = new Randomizer(),
    ) {}

    #[\Override]
    public function forTopic(string $topic): TopicPartitioner
    {
        return new RandomTopicPartitioner($this->randomizer);
    }
}
