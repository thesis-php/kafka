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
 * {@see Client::createProducer()}, wait for the records sent so far via
 * {@see Producer::flush()}, deliver the rest and release resources via
 * {@see Producer::close()}.
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
     * — some may succeed while others fail. A record that cannot be
     * queued, for example because its batch is full in manual-flush
     * mode, gets a Future that failed with the reason. Only
     * {@see Exception\ProducerClosed} and the cancellation of this call
     * leave a bulk call as an exception.
     *
     * When a call fills a batch, the next calls for this partition wait
     * until the partition takes this batch to send it. The calls for other
     * partitions and topics do not wait. A batch that the linger time or
     * {@see Producer::flush()} releases makes no call wait.
     *
     * The provided Cancellation aborts only the waits inside this call:
     * the producer id, the metadata of the topic and the wait for a slow
     * partition. When it fires, the call throws
     * {@see \Amp\CancelledException} and the record is not queued. Once the
     * record is appended to a batch, cancellation no longer affects it.
     *
     * @param Producer\Record|non-empty-list<Producer\Record> $records
     * @return ($records is non-empty-list<Producer\Record>
     *     ? non-empty-list<Future<Producer\Produced>>
     *     : Future<Producer\Produced>)
     * @throws Exception\ProducerClosed close() was called, also while this call waited
     * @throws Exception the record could not be queued; for a bulk call
     *                   the reason is in the Future of the record
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
     * The Cancellation works as in {@see Producer::produce()}, and it also
     * ends the wait for the delivery. The call then throws
     * {@see \Amp\CancelledException}.
     *
     * The records that were queued are sent even when this call throws.
     * To observe their result later, use {@see Producer::produce()} and
     * keep the Futures.
     *
     * @param Producer\Record|non-empty-list<Producer\Record> $records
     * @return ($records is non-empty-list<Producer\Record>
     *     ? non-empty-list<Producer\Produced>
     *     : Producer\Produced)
     * @throws Exception\ProducerClosed close() was called, also while this call waited
     * @throws Exception same as produce(), plus the first delivery
     *                   error of the records
     */
    public function produceSync(
        Producer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): Producer\Produced|array;

    /**
     * Send every open batch and wait until every record that was accepted
     * before this call has a result: the records in open batches, in the
     * queue and in flight. The delivery errors of records are not thrown
     * here. They are in the Futures of the records.
     *
     * After {@see Producer::flush()}, the producer is still operational — new records
     * can be produced normally. Use this between logical "phases" of
     * work when you want to make sure earlier records have landed
     * before continuing. For permanent shutdown, prefer {@see Producer::close()}.
     *
     * On a closed producer this method does nothing.
     */
    public function flush(): void;

    /**
     * Close the producer. From the moment of the call,
     * {@see Producer::produce()} and {@see Producer::produceSync()} throw
     * {@see Exception\ProducerClosed}. The open batches are sent at once,
     * without the linger time. The method waits until every accepted record
     * has a result, then stops the sender and closes the connections.
     *
     * Each attempt to send a batch has a time limit, see
     * {@see Producer\Config::$produceTimeout}. With retries this wait can
     * still be long. Give a Cancellation to limit it. When the Cancellation fires, the
     * records that are not delivered yet fail with
     * {@see \Amp\CancelledException}, and the producer is closed anyway.
     * A batch that was already written to the socket can still reach the
     * broker. The method does not throw in this case.
     *
     * The delivery errors of records are not thrown here. They are in the
     * Futures of the records.
     *
     * A call while another close() runs waits until the producer is closed.
     * Its Cancellation also stops the wait for the records. On a closed
     * producer this method returns at once.
     */
    public function close(Cancellation $cancellation = new NullCancellation()): void;
}
