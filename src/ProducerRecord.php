<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 *  A record to be sent to a Kafka topic.
 *
 *  The producer fills in fields the user didn't provide:
 *    - $topic:     left as null here is resolved against the producer's
 *                  default topic at send time. If neither is set, sending
 *                  the record fails — every record must end up bound to a topic.
 *    - $partition: left as null is resolved by the configured partitioner
 *                  (typically: hash of $key, or round-robin when $key is null).
 *    - $timestamp: left as null is filled with the current wall-clock time
 *                  in milliseconds at the moment the record is appended to
 *                  a batch. The broker may overwrite it when the topic is
 *                  configured with LogAppendTime.
 *
 * @api
 */
final readonly class ProducerRecord
{
    /**
     * @param ?string $key null is a valid record key (compacted-topic tombstone)
     * @param ?string $value null is a valid value (delete marker in compacted topics)
     * @param ?non-empty-string $topic null falls back to the producer's default topic
     * @param list<RecordHeader> $headers
     * @param ?non-negative-int $partition null means that the partitioner will decide
     * @param ?non-negative-int $timestamp in milliseconds
     */
    public function __construct(
        public ?string $key = null,
        public ?string $value = null,
        public ?string $topic = null,
        public array $headers = [],
        public ?int $partition = null,
        public ?int $timestamp = null,
    ) {}
}
