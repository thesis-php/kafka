<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
final readonly class ProduceResult
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param ?non-negative-int $offset
     * @param ?non-negative-int $timestamp
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public ?int $offset,
        public ?int $timestamp,
    ) {}
}
