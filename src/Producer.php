<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;

/**
 * A Producer buffers records by (topic, partition) into batches, sends
 * them to the partition leader, and resolves a Future for each record
 * with either a {@see Producer\Produced} or an error. Lifecycle: open via
 * {@see Client::createProducer()}, drain pending via {@see Producer::flush()}, release
 * resources via {@see Producer::close()}. Closing is idempotent.
 *
 * @api
 */
interface Producer
{
    /**
     * Hand a record (or a batch of records) over to the producer for
     * asynchronous delivery. The returned Future completes when the
     * broker acknowledges the write, or fails with an error if the
     * write could not be delivered after the configured retries.
     *
     * For {@see Producer\Acks::None} the broker doesn't send a response at all,
     * so the Future completes as soon as the write reaches the socket
     * and both offset and timestamp on the ProduceResult are null.
     *
     * For a bulk call, one Future per input record is returned in the
     * same order as the input list. Each Future resolves independently
     * — some may succeed while others fail.
     *
     * The provided Cancellation aborts only the metadata / partitioner
     * lookup that happens synchronously inside this call. Once the
     * record is appended to a batch, cancellation no longer affects it
     * — the only way to abandon an in-flight record is for the broker
     * to fail it.
     *
     * @param Producer\Record|non-empty-list<Producer\Record> $records
     * @return ($records is non-empty-list<Producer\Record>
     *     ? non-empty-list<Future<Producer\Produced>>
     *     : Future<Producer\Produced>)
     * @throws Exception the record's topic could not be resolved, or
     *                   the producer is closed; resolved Futures may
     *                   still carry per-record errors
     */
    public function produce(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Future|array;

    /**
     * Blocking counterpart to {@see Producer::produce()}: hands the record(s)
     * off and waits for delivery to complete. For a single record this
     * returns one ProduceResult; for a bulk call it returns a list
     * preserving input order.
     *
     * Unlike {@see Producer::produce()}, the supplied Cancellation here applies
     * to the wait as well — cancelling while waiting will throw out of
     * this call, but the records have already been buffered and will
     * be sent regardless. To observe their result later, use
     * {@see Producer::produce()} and keep the Futures.
     *
     * @param Producer\Record|non-empty-list<Producer\Record> $records
     * @return ($records is non-empty-list<Producer\Record>
     *     ? non-empty-list<Producer\Produced>
     *     : Producer\Produced)
     * @throws Exception same as produce(), plus any per-record
     *                   delivery error promoted from the Future
     */
    public function produceSync(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Producer\Produced|array;

    /**
     * Seal every currently-open batch and wait for all in-flight
     * batches to be acknowledged by their brokers. Returns when there
     * is nothing left to send and every previously-issued Future has
     * resolved.
     *
     * After {@see Producer::flush()}, the producer is still operational — new records
     * can be produced normally. Use this between logical "phases" of
     * work when you want to make sure earlier records have landed
     * before continuing. For permanent shutdown, prefer {@see Producer::close()}.
     *
     * @throws Exception
     */
    public function flush(): void;

    /**
     * Drain pending records (as {@see Producer::flush()} does), stop the sender
     * coroutine, and reject any subsequent {@see Producer::produce()} calls. Idempotent
     * — calling close() twice is a no-op the second time.
     *
     * @throws Exception
     */
    public function close(): void;
}
