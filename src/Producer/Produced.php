<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer;

/**
 * @api
 */
final readonly class Produced
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param ?non-negative-int $offset null when {@see Acks::None} (broker doesn't respond)
     * @param ?non-negative-int $timestamp null when topic uses CreateTime (broker doesn't override the timestamp
     *                                     the producer sent) or when {@see Acks::None}
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public ?int $offset,
        public ?int $timestamp,
    ) {}
}
