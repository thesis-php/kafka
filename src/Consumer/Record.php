<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

use Thesis\Kafka\RecordHeader;

/**
 * @api
 */
final readonly class Record
{
    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param non-negative-int $offset
     * @param list<RecordHeader> $headers
     * @param int $timestamp in milliseconds
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public int $offset,
        public ?string $key,
        public ?string $value,
        public array $headers,
        public int $timestamp,
    ) {}
}
