<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * The Producer calls forTopic() once per topic and reuses the returned
 * TopicPartitioner for that topic's lifetime. The split exists because
 * some implementations want to "stick" to one partition across many
 * records of the same topic — once a batch fills up there, rotate to
 * the next partition. Sticking lets batches reach a useful size
 * before they're sent, which improves throughput and compression
 * ratios; without it, round-robin and random partitioners spread
 * records thinly across partitions and every batch ships small.
 *
 * @see https://github.com/twmb/franz-go `kgo.Partitioner` and `kgo.TopicPartitioner` for reference.
 *
 * @api
 */
interface Partitioner
{
    /**
     * @param non-empty-string $topic
     */
    public function forTopic(string $topic): TopicPartitioner;
}
