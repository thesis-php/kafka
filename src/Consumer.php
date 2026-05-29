<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;

/**
 * Consumes records from the topics it was created for. Topics are fixed
 * at creation — to read different topics, create another consumer.
 *
 * Pull-based: {@see Consumer::consume()} returns whatever has been fetched, grouped into
 * per-partition batches. Commit is offset-based and group-only.
 *
 * @api
 */
interface Consumer extends ConsumerSession
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
}
