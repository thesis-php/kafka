<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\TopicPartition;

use Thesis\Kafka\Consumer\Internal\TopicPartition;

/**
 * SplObjectStorage that hashes TopicPartition by (topic, partition)
 * value rather than object identity. Lookups with a freshly-created
 * TopicPartition instance find the entry stored under any equal one.
 *
 * @template TValue
 * @template-extends \SplObjectStorage<TopicPartition, TValue>
 *
 * @internal
 */
final class Map extends \SplObjectStorage
{
    #[\Override]
    public function getHash(object $object): string
    {
        return $object->key();
    }
}
