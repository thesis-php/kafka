<?php

declare(strict_types=1);

namespace Thesis\Kafka\TopicPartition;

use Thesis\Kafka\TopicPartition;

/**
 * SplObjectStorage that hashes TopicPartition by (topic, partition)
 * value rather than object identity. Lookups with a freshly-created
 * TopicPartition instance find the entry stored under any equal one.
 *
 * @template TValue
 * @template-extends \SplObjectStorage<TopicPartition, TValue>
 *
 * @api
 */
final class Map extends \SplObjectStorage
{
    #[\Override]
    public function getHash(object $object): string
    {
        return $object->key();
    }

    /**
     * @return list<non-empty-string>
     */
    public function topics(): array
    {
        $topics = [];

        foreach ($this as $key) {
            $topics[] = $key->topic;
        }

        return $topics;
    }

    /**
     * @return iterable<array-key, TValue>
     */
    public function values(): iterable
    {
        foreach ($this as $key) {
            yield $this[$key];
        }
    }
}
