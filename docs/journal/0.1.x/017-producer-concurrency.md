# 017 Producer concurrency

Type: fixes

## Goal

A producer that is used by several coroutines at once never loses a record.
A slow partition delays only its own records. A Produce request that gets no
answer ends after a time limit, so a silent broker cannot hold a batch
forever.

## Context

- The findings are in `audit-findings.md`, section "017". Read it first.
  They were checked against the code when this task was planned. The note
  "Planned in task 017" at the end of the section says what is still true.
- The path of a record: `AsyncProducer::produce()`
  (`src/Producer/Internal/AsyncProducer.php`) gives it to a `TopicProducer`
  (`src/Producer/Internal/TopicProducer.php`), which keeps one open batch
  per partition. A released batch goes through one shared queue to `Sender`
  (`src/Producer/Internal/Sender.php`). Its router loop gives the batch to
  the `Sender\PartitionWorker` of the partition, which sends it with
  `Sender\BatchProducer`.
- The lost batch. `TopicProducer::queue()` releases a full batch, and
  `release()` can suspend in `push()`. Meanwhile a second `produce()` for
  the same partition opens a new batch and appends its record. The first
  call resumes and opens one more batch, which replaces the batch of the
  second call. Nothing holds the replaced batch any more. It is never sent,
  and the future of its record never resolves. With `idempotent` both
  batches also got the same base sequence.
- The blocked router. `Sender::run()` calls `PartitionWorker::enqueue()`,
  which is a `push()` into a queue without a buffer. The worker reads its
  queue only when it has a free in-flight slot
  (`Producer\Config::$maxInflightProduceRequest`). So when one partition is
  slow, the router waits for it and routes no batch of any other partition.
  The doc comment of `Sender::run()` says the opposite.
- This wait is also the only backpressure of the producer today: a
  `produce()` that releases a full batch waits until the router takes it.
- No time limit. `BatchProducer::send()` gives the cancellation of the
  worker to `Cluster::request()` (`src/Internal/Cluster.php`). A request
  with its own cancellation gets no client timeout there.
  `Producer\Config::$produceTimeout` is only sent to the broker as
  `timeoutMs` of the Produce request: the time the broker waits for the
  replicas.
- `Internal\RequestTimeout` (`src/Internal/RequestTimeout.php`) creates a
  client timeout that can be joined with the cancellation of a caller.
  `Internal\Connection` closes a connection that was silent for the whole
  time of such a timeout. The group coordinator already uses it with a
  longer limit (`src/Consumer/Internal/GroupConsumer/Coordinator.php`).
- A fired timeout reaches the caller as `Amp\CancelledException`.
  `BatchProducer::produce()` does not retry a `CancelledException`: since
  task 011 a cancelled batch is not sent again, and task 016 uses this for
  `close()` with a cancellation. So the timeout of the client and the
  cancellation of `close()` must be told apart.
- `AsyncProducer::produceSync()` gives its cancellation only to
  `produce()`. The wait for the delivery has no cancellation. The doc of
  `Producer::produceSync()` (`src/Producer.php`) says so since task 016.
- Two weak spots that nobody has seen on a broker:
  - `TopicProducer::release()` asks for the metadata of the topic when the
    partitioner is a `Producer\StickyTopicPartitioner`. It does this
    before the batch is marked as released. `release()` also runs in the
    callback of the linger timer. An exception there would reach the event
    loop. `Cluster::topic()` throws only when the topic is not in the
    metadata cache any more.
  - `PartitionWorker` does not handle an exception from
    `BatchProducer::produce()`. That method catches everything today, so
    nothing reaches this path. The worker also keeps its writes in a list
    that nothing reads.
- The tests of the producer are in `tests/ProducerTest.php`,
  `tests/ProducerCloseTest.php` and `tests/ProduceRetryTest.php`.
  `produceThatWaitsForQueueFailsAtClose` in `tests/ProducerCloseTest.php`
  shows how to make a `produce()` wait behind a stalled proxy.
- Java client: `request.timeout.ms` and `retries` in the
  [producer configuration](https://kafka.apache.org/documentation/#producerconfigs).

## What to do

Make the hand-over of a full batch safe for concurrent callers. After any
wait inside `produce()`, the call must work with the batch that is open for
the partition at that moment. It must not replace a batch that another call
opened. Each accepted record is in exactly one batch that is sent.

Make the routing independent of the state of a partition. The router never
waits for a busy partition. The batches of one partition are still sent in
the order of their release. The backpressure stays, but per partition: only
a `produce()` for the slow partition waits.

Give each Produce attempt a client time limit. An attempt that runs out of
time is a failed attempt that can be retried, like a lost connection. The
cancellation of `close()` still ends the batch without a retry.

Let the cancellation of `produceSync()` also end the wait for the delivery.

Update the doc comments of `Producer`, `Producer\Config` and `Sender` so
that they say what the code really does.

## Decisions

- No signature of an existing `@api` item changes.
- Time limit of one Produce attempt:
  `Producer\Config::$produceTimeout` + `Config::$requestTimeout`. The
  broker can use the whole `produceTimeout`, and the rest is for the
  network. The limit starts when the attempt starts.
- An attempt without an answer in this time is retried like a network
  error. It counts as an attempt of `Producer\Config::$produceAttempts`,
  with the same backoff and metadata refresh as other retries.
- A connection that was silent for the whole limit is closed, by the
  existing logic of `Internal\RequestTimeout` and `Internal\Connection`.
- New `@api` class `Thesis\Kafka\Exception\ProduceTimedOut`. It extends
  `Thesis\Kafka\NetworkException`. Public readonly properties: `topic`
  (`non-empty-string`) and `partition` (`non-negative-int`), as in
  `Exception\NoAvailablePartitionLeader`. When the last attempt runs out of
  time, the futures of the batch fail with it.
- Without `idempotent`, a retry after a timeout can write the batch twice.
  This is the same as for any retry today. The doc of
  `Producer\Config` says it.
- `Producer\Acks::None` has no answer, so there is no wait and no timeout.
- A cancellation of `close()` still fails the batch with
  `Amp\CancelledException` and does not retry it, as task 016 decided.
- Backpressure is per partition. A `produce()` that releases a full batch
  waits until the partition can take the batch. A `produce()` for another
  partition or topic does not wait for it.
- The cancellation of `produce()` ends this wait with
  `Amp\CancelledException`. `close()` ends it with
  `Exception\ProducerClosed`, as today. In both cases the record of the
  call is not queued. The full batch that the call released is still sent.
- A batch that is released by the linger timer or by `flush()` makes no
  caller wait, as today.
- With `maxInflightProduceRequest: 1` the batches of one partition reach
  the broker in the order of their release.
- `produceSync()`: its cancellation also ends the wait for the delivery.
  The call then throws `Amp\CancelledException`. The records that were
  queued are still sent, and nothing reaches the event loop from their
  futures. `produce()` does not change: once a record is in a batch, the
  cancellation of `produce()` does not affect it.
- The two weak spots of the "Context" follow the rule of
  `audit-findings.md`:
  - The throw in the linger timer: first try to show it with a test. When
    a test shows it, fix it: no exception leaves the timer callback, and
    the batch is still sent or its futures fail. When no test can show it,
    do not change the code and say so in the report.
  - `PartitionWorker`: no test can reach this path. Do not add a `catch`
    that no test covers. Remove state that nothing reads, if the new
    design does not need it.
- No `\LogicException` or `\Error` reaches the user from the paths of this
  task.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

The proxy tests run on `Kafka::PROXIED_BROKER` with `proxiedProducer()`.
A stall of the proxy keeps a batch in flight. Extend the kit when a test
needs something it does not have, for example a shorter request timeout
for `proxiedProducer()`.

- Concurrent produce to one partition: the first batch is in flight behind
  a stalled proxy, and more full batches wait, until one `produce()` call
  waits. A second `produce()` for the same partition starts while the first
  one waits. The proxy drops the connections. Both calls return, every
  future has an offset, and a consumer reads every record exactly once.
- The same scenario with `idempotent: true`: every future has an offset
  and no future fails with a sequence error.
- Slow partition does not block another one: a topic with two partitions.
  Partition 0 has a batch in flight behind a stalled proxy and several
  batches behind it. A record goes to partition 1. The log has a produce
  attempt for partition 1 while the batches of partition 0 still wait.
  After the proxy drops the connections, every record is delivered.
- Slow partition does not block a `produce()` for another one: in the
  state where a `produce()` for partition 0 waits, a `produce()` for
  partition 1 returns its future without a wait.
- Order in a partition: several batches of one partition wait behind a
  stalled proxy. After the drop a consumer reads the records in the order
  they were produced.
- Cancellation of a waiting `produce()`: a `produce()` waits because its
  partition is slow. Its cancellation fires. The call throws
  `CancelledException`. After the drop the other records are delivered and
  the record of this call is not in the topic.
- Timeout, then success: short `produceTimeout` and `requestTimeout`. The
  proxy stalls, a record is produced. The first attempt runs out of time.
  The next attempt uses a new connection and delivers the record. The log
  has two attempts for the batch.
- Timeout on every attempt: `produceAttempts: 2` and a proxy that stalls
  again before each new attempt, or another way that keeps every attempt
  silent. The future fails with `ProduceTimedOut` that names the topic and
  the partition. The log has two attempts.
- No early timeout: with a `requestTimeout` shorter than the time the
  broker needs, but `produceTimeout` + `requestTimeout` longer, the record
  is delivered with one attempt. Use it only if a test can make the broker
  slow in a stable way; otherwise say in the report that it was not tested.
- `close()` with a cancellation during an attempt still fails the record
  with `CancelledException` and one attempt. This is the existing test
  `closeWithCancellationFailsUndeliveredRecords`; it must pass unchanged.
- `produceSync()` with a cancellation: the proxy stalls, `produceSync()`
  runs with a cancellation that fires. It throws `CancelledException`, and
  no error reaches the event loop (`loopErrorsDuring()`). After the drop a
  consumer reads the record.
- `produceSync()` with a list and a cancellation: the same, with two
  records.
- The linger timer: try a sticky partitioner, a topic that is deleted
  while a batch is open, and a short `metadataMaxAge`. See "Decisions".
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] Two concurrent `produce()` calls for one partition never lose a
      record, also when one of them waits.
- [ ] A slow partition does not delay the batches or the `produce()` calls
      of other partitions.
- [ ] A Produce attempt ends after `produceTimeout` + `requestTimeout` and
      is retried.
- [ ] After the last attempt without an answer the future fails with
      `ProduceTimedOut`.
- [ ] The cancellation of `produceSync()` ends the wait for the delivery.
- [ ] The doc comments of `Producer`, `Producer\Config` and `Sender` match
      the behaviour.
- [ ] `README.md` says what `produceTimeout` and `produceAttempts` mean
      for a broker that does not answer.
- [ ] The report says what happened with the two weak spots.
- [ ] `make check` passes.

## Out of scope

- A total time limit for the delivery of a record (delivery timeout).
- A memory limit for buffered records (`buffer.memory` of the Java
  client).
- Sequence numbers of the idempotent producer after a failed or timed out
  batch, and `DuplicateSequenceNumber` (task 018).
- One Produce request per broker (task 027).
- A batch is encoded again on every attempt (task 026).
- The `\LogicException` "No producer record topic and no default"
  (task 031).

## Depends on

016
