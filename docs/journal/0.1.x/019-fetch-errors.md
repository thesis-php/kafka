# 019 Fetch errors

Type: fixes

## Goal

A consumer reacts to every error of a Fetch. It resets an offset that is
out of range, follows a partition to its new leader, waits before it tries
again, and throws an error that it cannot fix from `consume()`. Today such
errors are only logged: the consumer spins, stays on an old leader, or
waits forever without a word.

## Context

- The findings are in `audit-findings.md`, section "019". Read it first.
  They were checked against the code when this task was planned. The note
  "Planned in task 019" at the end of the section says what is still true,
  what is fixed already and what moved to task 045.
- Both kinds of consumer read through `Consumer\Internal\AssignmentConsumer`
  (`src/Consumer/Internal/AssignmentConsumer.php`). `start()` asks the
  metadata for the leader of each partition and starts one
  `Consumer\Internal\Fetcher` per leader. Each `Fetcher` runs a loop: build
  a Fetch request for the partitions of its broker, send it, decode the
  answer, push the records into the queue that `consume()` reads.
- `Consumer\Internal\SubscriptionState` keeps the position and the leader
  of each partition. The leader is set once, in `assign()`. Nothing changes
  it later, and a `Fetcher` is bound to one broker id. So after a leader
  moves, the partition is still fetched from the old broker.
- `Fetcher::dispatch()` only logs the error code of a partition and goes
  on. The next request is sent at once, with the same position. With
  `OffsetOutOfRange` this is a loop without a pause, and
  `Consumer\Config::$autoOffsetReset` is never used after the start.
- `Fetcher::run()` catches the exceptions of the request only. `dispatch()`
  is outside the `try`. `Exception\UnsupportedCodec`,
  `Exception\CorruptRecordBatch`, `Exception\UnexpectedEof` and the others
  of `Consumer\Internal\Protocol\RecordBatch::decode()` end the loop. So
  does an exception of `Cluster::refreshMetadata()` inside the `catch`.
  Nobody looks at the future of the loop, so `consume()` waits forever. A
  group member keeps its partitions and reads nothing.
- The `catch` of `Fetcher::run()` has no pause either: a broker that is
  down gives one failed request and one metadata refresh after the other.
- A Fetch has no client time limit: `Fetcher::fetch()` gives the
  cancellation of the consumer to `Cluster::request()`, and a request with
  its own cancellation gets no client timeout. A half-open connection is
  not found. `Internal\RequestTimeout::start()` joins a client timeout with
  the cancellation of a caller; `Sender\BatchProducer` uses it this way
  since task 017. `OffsetLister` (`src/Consumer/Internal/OffsetLister.php`)
  has the same gap when a group consumer calls it.
- The direct consumer (`src/Consumer/Internal/DirectConsumer.php`) asks
  `OffsetLister` for the start offsets. `OffsetLister` sends
  `Consumer\AutoOffsetReset::$value` as the timestamp. For
  `AutoOffsetReset::None` this is 0, and the consumer reads from the start
  without a word. The group consumer handles `None` in
  `GroupConsumer\OffsetResolver`: it throws
  `Exception\NoOffsetForPartitions`.
- An exception that the fetch core gives to its queue reaches the user:
  from `consume()` of a direct consumer, and through
  `GroupConsumer\GroupSession::run()` and `GroupConsumer::run()` from
  `consume()` of a group consumer. The group member then leaves the group.
- `ErrorCode::retriable()` (`src/Protocol/ErrorCode.php`) is the list of
  the errors that a new attempt can fix. `Internal\Retry\backoff()` gives
  the pause before an attempt, up to 1 s.
- "`ListOffsets` errors are a bare `\RuntimeException`" of the findings is
  fixed already: `OffsetLister` throws `Exception\ProtocolError` and
  retries.
- Java client: `auto.offset.reset` in the
  [consumer configuration](https://kafka.apache.org/documentation/#consumerconfigs).

## What to do

Give every result of a Fetch one of three reactions.

An offset that is out of range is reset by the policy of the consumer. The
consumer asks the broker for the earliest or the latest offset of the
partition and reads on from there.

An error that a new attempt can fix is tried again, after a pause and with
new metadata. When the leader of a partition has moved, the partition is
read from the new leader, from the same position. A partition with such an
error does not stop the other partitions of the consumer.

Every other error stops the consumer and leaves `consume()` as an
exception. This is also true for an exception anywhere in the fetch loop:
no path may end a loop without a word.

Give Fetch and ListOffsets a client time limit, so a silent connection is
found and closed.

Reject a direct consumer with `AutoOffsetReset::None` when it is created.

## Decisions

- New `@api` class `Thesis\Kafka\Exception\OffsetOutOfRange`. It extends
  `Thesis\Kafka\Exception`. Public readonly properties: `partition`
  (`Thesis\Kafka\TopicPartition`) and `offset` (`non-negative-int`), the
  position that the broker rejected.
- `OffsetOutOfRange` from the broker:
  - with `AutoOffsetReset::Earliest` or `AutoOffsetReset::Latest` the
    consumer asks for this offset with ListOffsets and reads on from it.
    Nothing is thrown;
  - with `AutoOffsetReset::None` the consumer stops and `consume()` throws
    `Exception\OffsetOutOfRange`.
- Errors that are tried again, without a limit of attempts: every error
  code of `ErrorCode::retriable()`, a `Thesis\Kafka\NetworkException`, the
  time limit of the request, `Exception\UnknownBroker`,
  `Exception\NoAvailablePartitionLeader` and `Exception\UnknownTopic` of
  the local metadata. The user limits the wait with the cancellation of
  `consume()` or with `close()`.
- Before each new attempt there is a pause of `Internal\Retry\backoff()`
  and a metadata refresh. No path sends requests in a loop without a
  pause. A failed metadata refresh is tried again in the same way.
- After the refresh each partition is read from its current leader, from
  the position it had. No record is lost and no record is delivered twice
  because of the move.
- A partition that waits for a new attempt does not stop the others:
  records of the other partitions in the same answer are delivered, and
  the other partitions are fetched on.
- Every other error code of a partition stops the whole consumer:
  `consume()` throws `Exception\ProtocolError` with a
  `Protocol\Error\PartitionError` that names the topic, the partition and
  the code.
- An exception of the decoding (`UnsupportedCodec`, `CorruptRecordBatch`,
  `UnsupportedMessageFormat`, `UnexpectedEof`, `CompressionFailed`) stops
  the whole consumer: `consume()` throws it unchanged.
- After such a stop no partition is fetched any more. A group member
  leaves the group, as today. What a later `consume()` on this consumer
  does is a part of task 020.
- `Client::consume()` with a callback: the error reaches
  `ConsumerSubscription::join()`, as today for an error of the handler.
- Time limit of one Fetch attempt: `Consumer\Config::$maxWait` +
  `Config::$requestTimeout`. Time limit of a ListOffsets request:
  `Config::$requestTimeout`. Each is joined with the cancellation of the
  caller. A fired limit is an error that is tried again. A connection that
  was silent for the whole limit is closed by the existing logic of
  `Internal\RequestTimeout` and `Internal\Connection`.
- `Client::createConsumer()` and `Client::consume()` throw
  `Exception\InvalidConfiguration` when the config has no group and
  `autoOffsetReset` is `AutoOffsetReset::None`. No request is sent. This
  is the only change of the behaviour of an existing `@api` method. Add
  the `@throws` to both and say it in the doc of `AutoOffsetReset::None`.
- No `\LogicException`, `\RuntimeException` or `\Error` reaches the user
  from the paths of this task.
- The length check in `Protocol\RecordBatch::decode()` is not changed
  without a test that shows a problem.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

- Out of range, `Earliest`: a group has a committed offset behind the end
  of the log (set it with a raw request). A group consumer with
  `AutoOffsetReset::Earliest` reads the records of the topic from the
  first one.
- Out of range, `Latest`: the same setup with `AutoOffsetReset::Latest`.
  The consumer reads only a record that is produced after its reset.
- Out of range, `None`: the same setup with `AutoOffsetReset::None`.
  `consume()` throws `OffsetOutOfRange` with the partition and the
  committed offset.
- Out of range while it reads: a consumer has read some records and waits.
  The start of the log moves past its position, for example with a raw
  DeleteRecords request after a rewind of the committed offset, or another
  way that shows it. The consumer goes on by its policy.
- No spin: in the `Earliest` scenario the log has a small, fixed number of
  fetch errors for the partition, not one per loop turn.
- Leader moves: a topic with replicas on the main cluster. A consumer
  reads some records. The leader of the partition moves to another broker
  (a raw request, for example a reassignment or a leader election). New
  records are produced. The consumer reads every record once, in order.
  Run it for a direct consumer and for a group consumer. If no raw request
  can move a leader on the test cluster, stop and report it.
- Another partition goes on: a topic with two partitions whose leaders are
  different brokers. While one partition waits for a new attempt, records
  of the other partition are delivered.
- Unknown codec: a producer writes with a compressor. A consumer whose
  `decompressors` do not have this codec gets `UnsupportedCodec` from
  `consume()`, for a direct consumer and for a group consumer. No error
  reaches the event loop (`loopErrorsDuring()`). The member of the group
  consumer has left the group.
- Unknown codec with a callback: `Client::consume()` with the same setup.
  `ConsumerSubscription::join()` throws `UnsupportedCodec`.
- Silent connection: on `Kafka::PROXIED_BROKER` behind a proxy, a direct
  consumer with a short `maxWait` and a short request timeout reads a
  record. The proxy stalls. After the time limit the consumer uses a new
  connection (`Proxy::accepted()` grows) and reads a record that is
  produced then.
- Broker is gone: the proxy disconnects for a few seconds. The log shows
  attempts with pauses between them, not a burst. After the proxy is back
  the consumer reads a new record.
- Deleted topic: a consumer reads two topics. One topic is deleted. The
  consumer still reads new records of the other topic.
- `AutoOffsetReset::None` without a group: `Client::createConsumer()` and
  `Client::consume()` throw `InvalidConfiguration`. With a group they do
  not.
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] `OffsetOutOfRange` is reset by `autoOffsetReset`, or thrown with
      `None`.
- [ ] A consumer reads on after the leader of a partition has moved.
- [ ] No error makes a fetch loop send requests without a pause.
- [ ] An error that cannot be fixed leaves `consume()` as an exception. No
      fetch loop ends without a word.
- [ ] Fetch and ListOffsets have a client time limit.
- [ ] A direct consumer with `AutoOffsetReset::None` cannot be created.
- [ ] `README.md`, section "Consumer", says what the consumer does with
      errors of a fetch and what `autoOffsetReset` means.
- [ ] `make check` passes.

## Out of scope

- A Fetch that is cancelled at a rebalance and holds its connection until
  `maxWait` ends, and the order of the partitions in a Fetch request
  (task 045).
- The state of a consumer after a failed or cancelled `consume()`, and
  batches that are delivered after `close()` (task 020).
- A rebalance of the group when a topic is deleted or gets new partitions
  (tasks 023 and 035).
- A way for the user to skip a bad record or to seek (task 034).
- Fetch sessions and a fetch from the closest replica (deferred to 0.2.x).
- `\LogicException` in `AssignmentConsumer` and `GroupConsumer`
  (task 031).

## Depends on

nothing
