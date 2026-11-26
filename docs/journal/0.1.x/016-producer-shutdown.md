# 016 Producer shutdown

Type: fixes

## Goal

`close()` of a producer delivers every record that was accepted, releases
every connection and timer, and leaves the producer in a clear closed state.
`flush()` waits for every record that was accepted before it. A record is
never lost without a result: its future always resolves.

## Context

- The findings are in `audit-findings.md`, section "016". Read it first.
  They were checked against the code when this task was planned. The note
  "Planned in task 016" at the end of the section says what is still true.
- The public interface is `src/Producer.php` (`@api`). The implementation is
  `src/Producer/Internal/AsyncProducer.php`. `Client::createProducer()`
  (`src/Client.php`) creates a new `Internal\Cluster` for each producer, so
  the producer owns it.
- The path of a record: `AsyncProducer::produce()` gives it to a
  `TopicProducer` (`src/Producer/Internal/TopicProducer.php`), which keeps
  one open batch per partition. A batch is released when it is full, when
  the linger timer fires, or on `flush()`. A released batch goes through a
  queue to `Sender` (`src/Producer/Internal/Sender.php`), then to the
  `Sender\PartitionWorker` of its partition, which sends it with
  `Sender\BatchProducer`.
- `close()` today waits only for the batches that are still open. A batch
  that was released before and waits for the answer of the broker is
  cancelled: `PartitionWorker::stop()` cancels the writes. The futures of
  its records fail with `Amp\CancelledException`. The doc of
  `Producer::close()` says "drain pending records".
- `flush()` has the same gap: `TopicProducer::flush()` waits only for the
  batches that were open when it was called. The doc of `Producer::flush()`
  says that it returns when every earlier future has resolved.
- There is no closed state for `produce()`. After `close()`, a `produce()`
  to a known topic throws `\LogicException` ("TopicProducer is dead"). A
  `produce()` to a new topic creates a `TopicProducer` on a completed queue.
  Later the event loop gets "Values cannot be enqueued after calling
  complete".
- A `produce()` can wait for the metadata or for the producer id when
  `close()` is called. After the wait it opens a batch and starts a linger
  timer on a closed producer.
- `TopicProducer::release()` and `PartitionWorker::enqueue()` call `push()`
  on a queue without a cancellation. The caller waits forever when nobody
  reads the queue. Do not use `dispose()` after `complete()` to release such
  callers: it does nothing in `amphp/pipeline` 1.2.0 and 1.2.1, and
  `composer.json` allows both.
- `AsyncProducer::close()` does not close its `Cluster`. The sockets and the
  metadata timer stay in the event loop. `Cluster::close()` exists and is
  idempotent.
- `IdempotencyCoordinator::initialize()`
  (`src/Producer/Internal/IdempotencyCoordinator.php`) shares one
  InitProducerId request between the callers. When the request fails, the
  shared future gets the error and nobody awaits it, so the event loop gets
  an `UnhandledFutureError`. The shared request runs with the cancellation
  of the first caller only.
- `AsyncProducer::produce()` with a list calls `doProduce()` for each
  record. When one record throws, for example `Exception\BatchFull` with
  `manualFlush`, the records before it are already queued, but the caller
  gets only the exception and no futures.
- The client has no time limit for a Produce request (section 017,
  "`produceTimeout` is not enforced"). So a wait for a silent broker has no
  end. This is why `close()` gets a cancellation in this task.
- Since task 011 a cancelled batch is not sent again.
  `tests/ProduceRetryTest.php` has two tests that close a producer to
  cancel a batch: `cancelledProduceIsNotRetried` and
  `produceCancelledDuringBackoffIsNotRetried`.
- The test kit closes every producer of `$kafka->producer()` at the end of
  a scenario (`tests/Testing/Kafka.php`).

## What to do

Give the producer one closed state that every public method checks, and one
shutdown path.

`close()` stops new records, releases the open batches, and waits until
every accepted record has a result. Then it stops the sender and closes the
cluster. When its cancellation fires, it stops waiting: the batches that are
not delivered yet fail, and the resources are released anyway.

`flush()` waits for every record that was accepted before the call: in an
open batch, in a queue, or in flight.

A `produce()` on a closed producer, or one that waits when the producer is
closed, fails with a library exception and leaves nothing behind: no batch,
no timer, no push into a completed queue.

A bulk `produce()` always returns one future per record.

A failed InitProducerId request reaches the callers as an exception and
nothing reaches the event loop.

Update the doc comments of `Producer` so that they say what the methods
really do.

## Decisions

- New signature of the `@api` method:
  `Producer::close(Cancellation $cancellation = new NullCancellation()): void`.
  This is the only signature change.
- `close()` waits for all accepted records: open batches, queued batches
  and batches in flight. It does not wait for the linger time. It returns
  when every future of the producer has resolved, the sender has stopped
  and the cluster is closed.
- When the cancellation of `close()` fires: the records that are not
  delivered yet fail with `Amp\CancelledException`. `close()` still stops
  the sender and closes the cluster, and then returns without an exception.
  A batch that was already written to the socket can still reach the
  broker.
- The delivery errors of records do not leave `close()`. They are in the
  futures of the records.
- A second `close()` call while the first one still runs waits until the
  producer is closed. A cancellation of any of these calls forces the
  close, as described above. `close()` on a closed producer returns at
  once.
- New `@api` class `Thesis\Kafka\Exception\ProducerClosed`, like
  `Exception\ConsumerClosed`.
- `produce()` and `produceSync()` on a closed producer throw
  `ProducerClosed` from the call. This is true from the moment `close()`
  is called, not from the moment it returns. The record is not queued.
- A `produce()` that waits for the metadata or for the producer id when
  `close()` is called throws `ProducerClosed`. The record is not queued.
- `flush()` on a closed producer does nothing and returns.
- `flush()` waits for every record accepted before the call. It does not
  throw the delivery errors of records. It has no cancellation parameter.
- Bulk `produce([...])`: a record that cannot be queued gets a future that
  failed with the exception, for example `BatchFull` or `UnknownPartition`.
  The other records of the list are handled as usual. The call returns one
  future per record, in the order of the input.
- Two exceptions still leave a bulk call as an exception: `ProducerClosed`
  and `Amp\CancelledException` of the cancellation given to `produce()`.
- A single `produce($record)` throws as before.
- `produceSync()` with a list keeps its behaviour: it throws the first
  error of the futures.
- InitProducerId: when the request fails, every caller that waits for it
  gets the exception, and the next `produce()` starts a new request. No
  `UnhandledFutureError` reaches the event loop. A caller whose
  cancellation fires gets `CancelledException`; the other callers that
  wait for the same producer id are not failed by it. `close()` ends the
  request.
- No `\LogicException` or `\Error` reaches the user from the paths of this
  task.
- The test kit may close its producers with a time limit, so a scenario
  with a silent broker still ends.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

- Close waits for a batch in flight: on `kafka-proxied-standalone` behind a
  proxy, a record is produced and its batch is in flight (the proxy
  stalls). `close()` runs in the background and does not return. The proxy
  drops the connections, the retry delivers the record, and then `close()`
  returns. The future has the offset.
- Close with a cancellation: the same setup, the proxy stays silent.
  `close()` with a cancellation that fires returns without an exception.
  The future of the record fails with `CancelledException`, and the log has
  one produce attempt for the batch.
- Close releases everything: after `produceSync()` and `close()` the event
  loop has no work of the producer left (`loopWorkAfter()`): no socket, no
  metadata timer.
- Close and many batches: several records go to several partitions, some
  batches are released by size before `close()`. After `close()` every
  future has an offset, and a consumer reads all records.
- Produce after close: `produce()` and `produceSync()` throw
  `ProducerClosed`, for a topic the producer used before and for a new
  topic. Nothing reaches the event loop (`loopErrorsDuring()`).
- Flush after close returns. A second `close()` returns.
- Two `close()` calls at once: the second returns only when the first has
  finished (use the stalled proxy to keep the first one waiting).
- Produce that waits at close: a `produce()` for a topic waits for the
  metadata (a stalled proxy on the bootstrap path, or another way that
  shows it), then the producer is closed. The call throws `ProducerClosed`,
  and no error reaches the event loop after the linger time.
- Flush waits for released batches: with a short linger, a batch is
  released and in flight behind a stalled proxy. `flush()` runs in the
  background and does not return until the proxy drops the connections and
  the record is delivered.
- Bulk produce with `manualFlush`: a list where one record does not fit
  into the batch. The call returns one future per record. The future of
  that record fails with `BatchFull`. After `flush()` the other futures
  have offsets.
- Bulk produce with a wrong partition: one record of the list names a
  partition that the topic does not have. Its future fails with
  `UnknownPartition`, the others are delivered.
- InitProducerId fails: an idempotent producer whose broker is not
  reachable (a disconnected proxy). Two `produce()` calls at once both get
  the network exception, and no `UnhandledFutureError` reaches the event
  loop. After the proxy works again, a new `produce()` is delivered.
- The two tests of `tests/ProduceRetryTest.php` that close a producer to
  cancel a batch are changed to the new `close()` and still show that a
  cancelled batch is not sent again.
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] `close()` returns only when every accepted record has a result, or
      when its cancellation fires.
- [ ] After `close()` the producer holds no socket and no timer.
- [ ] `flush()` waits for batches in flight.
- [ ] `produce()` after `close()` throws `ProducerClosed`, and nothing
      reaches the event loop.
- [ ] A bulk `produce()` returns one future per record.
- [ ] A failed InitProducerId request leaves no `UnhandledFutureError`.
- [ ] The doc comments of `Producer` match the behaviour.
- [ ] `README.md` says how to close a producer, with and without a time
      limit.
- [ ] `make check` passes.

## Out of scope

- A client time limit for a Produce request and `produceTimeout`
  (task 017).
- A concurrent `produce()` that loses a batch, and one slow partition that
  blocks the others (task 017).
- `PartitionWorker` and an exception from `BatchProducer::produce()`
  (task 017).
- Sequence numbers of the idempotent producer after a failed or cancelled
  batch (task 018).
- A delivery timeout for a record.
- `Client::close()` and the producers of a client: a producer is closed by
  its own `close()`.

## Depends on

011
