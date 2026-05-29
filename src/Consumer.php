<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\NullCancellation;

/**
 * Consumes records from the topics it was created for. Topics are fixed
 * at creation — to read different topics, create another consumer.
 *
 * Pull-based: {@see Consumer::consume()} returns whatever has been fetched, grouped into
 * per-partition batches. Commit is offset-based and group-only.
 *
 * @api
 */
interface Consumer
{
    /**
     * Yield record batches from the consumer's internal buffer. Each
     * yielded item is a non-empty list of records belonging to a single
     * (topic, partition), in offset order.
     *
     * The shape of the returned iterable depends on $mode — see
     * {@see Consumer\ConsumeMode} for the difference between a one-shot batched
     * read and a continuous stream.
     *
     * The $cancellation bounds the wait. When null:
     *  - In {@see Consumer\ConsumeMode::Poll}, the consumer's pollTimeout (from
     *    config) is used as a default wait bound.
     *  - In {@see Consumer\ConsumeMode::Stream}, the stream runs indefinitely until
     *    {@see close()} is called.
     *
     * Passing an explicit Cancellation overrides this in both modes.
     *
     * Only one {@see consume()} may be active at a time: the consumer drives a
     * single cursor over its buffer, so a second concurrent {@see consume()} would
     * split batches with the first. Calling {@see consume()} while another is
     * running throws {@see Exception\ConsumerBusy}. Calling it on a closed consumer
     * throws {@see Exception\ConsumerClosed}.
     *
     * @return iterable<non-empty-list<Consumer\Record>>
     * @throws Exception\ConsumerClosed
     * @throws Exception\ConsumerBusy another {@see consume()} is already running
     * @throws Exception
     */
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable;

    /**
     * Commit progress to the group coordinator. Accepts a single record
     * or a list of records; for each (topic, partition) involved, the
     * committed offset is set to the highest record offset + 1 (the next
     * offset to read).
     *
     * Because offsets are linear, committing implies every record up to
     * that offset was processed — make sure you've handled the full
     * prefix, not a sparse subset, or the gaps will be treated as done.
     *
     * Group mode only.
     *
     * @param Consumer\Record|non-empty-list<Consumer\Record> $records
     * @throws Exception\NoGroupConfigured
     */
    public function commit(
        Consumer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): void;

    /**
     * Leave the group (if any), stop fetching, release resources.
     * Idempotent.
     */
    public function close(): void;
}
