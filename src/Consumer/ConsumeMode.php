<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

namespace Thesis\Kafka\Consumer;

/**
 * How {@see Consumer::consume()} should yield record batches.
 *
 * @api
 */
enum ConsumeMode
{
    /**
     * One-shot batched read.
     *
     * Waits up to the cancellation's deadline for at least one record
     * batch to arrive, then returns immediately with whatever is in the
     * internal buffer beyond the first. The returned iterable is finite:
     * iterating it walks the captured snapshot and ends. If the deadline
     * elapses with nothing arriving, the iterable is empty.
     *
     * Suits a processing loop where each iteration handles a bounded
     * chunk of work and the caller decides when to ask for more — for
     * example, "read a chunk, process it, commit, repeat". Pair with a
     * {@see TimeoutCancellation} to bound the wait.
     */
    case Poll;

    /**
     * Continuous stream.
     *
     * The returned iterable runs indefinitely: it yields batches as they
     * arrive and suspends while the buffer is empty, resuming when more
     * data is pushed in. Ends only when the consumer is closed or the
     * provided cancellation fires.
     *
     * Suits a long-running consumer that processes records as a flow,
     * without natural "chunks" — for example, a daemon that handles each
     * record and never returns control to a higher-level loop.
     */
    case Stream;
}
