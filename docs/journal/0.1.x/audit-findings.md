# Audit findings (2026-11-25)

Raw findings of a code audit of `src/` (without `src/Protocol/Generated/`).
They are grouped by the task that will fix them. `/plan-task` uses this file
to write the task files.

How to read this file:

- The findings come from reading the code. Nobody reproduced them against a
  broker. The only measured one is the varint decoding cost (task 010).
- Line numbers are from commit `7342f14`. They move when the code changes.
- "Confidence" is the confidence of the auditor in the mechanism, not in how
  often it happens.
- Rule for every bug below: first write an integration test that fails, then
  fix. If a bug cannot be reproduced through the public API, do not fix it
  blindly. Put it in the task report.
- Notes that start with "Found in task NNN" were added later, while that
  task was implemented. They were seen on a running broker or in the code of
  that day. See "Findings outside a task" in `AGENTS.md`.

## 010 Varint and int64 without copies

- **Every varint read copies the rest of the buffer.**
  `src/Protocol/Reader.php:110-131`: `substr($this->buffer, $this->offset)`
  before each varint. Decoding is quadratic. Measured with `Record::decode`
  on records of about 120 bytes: 1,000 records 0.009 s; 10,000 records
  0.28 s; 40,000 records (4.85 MB) 17.75 s. Encoding 40,000 records takes
  0.19 s. Any large flexible response is hit too (Metadata, Fetch).
  Confidence: high, measured.
- **int64 and varint use `BcMath\Number`.** `src/Protocol/Reader.php:86`,
  `src/Protocol/Writer.php:47, 67, 72-74`. Measured: 100k `readInt64` 0.046 s
  against 0.004 s for `unpack('J')`. A record has 6 or more varints.
  Confidence: high.
- **Truncated or malformed varints leave the exception model.**
  `src/Protocol/Reader.php:112-119, 124-130`. At the end of the buffer the
  result is `AssertionError`, or `Thesis\Varint\MalformedVarintNumber` with
  assertions off. It is not a `Thesis\Kafka\Exception`, and its message holds
  the raw buffer. There is no cap of 5 or 10 bytes. `Connection` treats only
  `MalformedResponse` as a decode failure. `Reader::peek()` (line 400) throws
  a bare `\OutOfBoundsException`. Confidence: high.
- Unverified: `Writer::writeString()` (`src/Protocol/Writer.php:77-85`)
  guards a string over 32,767 bytes only with an `assert`.

## 011 Retriable errors and leader change

- **`ErrorCode::retriable()` is not complete.**
  `src/Protocol/ErrorCode.php:289-319`. Missing, compared with Kafka:
  `NotLeaderForPartition` (6), `ReplicaNotAvailable` (9), `NotCoordinator`
  (16), `PreferredLeaderNotAvailable` (80), `InconsistentTopicID` (103).
  Listed but not retriable in Kafka: `BrokerNotAvailable` (8). Confidence:
  high.
- **A leader change fails the batch.**
  `src/Producer/Internal/Sender/BatchProducer.php:90-134`. Error 6 is not
  retriable, so the batch fails on the first response. Exceptions
  (`ConnectionClosed`, `UnknownBroker`, `NoAvailablePartitionLeader`) go to
  the `catch`, which does not refresh metadata. No path waits between
  attempts, so 10 attempts run back to back. Confidence: high.
- `BatchProducer.php:73` uses `ErrorCode::from()`. It throws `ValueError` for
  a code that the enum does not know. `Errors::code()` has the safe form.
- Found in task 013: two items of this section are done there.
  `NotLeaderForPartition` is retriable, and `BatchProducer` waits and
  refreshes the metadata between two attempts after a retriable error. Task
  008 replaced `ErrorCode::from()` in `BatchProducer`. Still open: the other
  error codes, and the `catch` path of `BatchProducer` (`ConnectionClosed`,
  `UnknownBroker`, `NoAvailablePartitionLeader`) that does not refresh the
  metadata.
- Found in task 008: the retry closure in
  `src/Producer/Internal/IdempotencyCoordinator.php` still uses
  `ErrorCode::from()`.

## 012 Generator: client versions and null structs

- **Broker-only versions are offered to the client.**
  `generator/Generator.php:98-105`,
  `src/Protocol/Generated/ApiKey.php:228-230`: `AddPartitionsToTxn` has the
  range 0-5. The schema says that clients use version 3 and below. At version
  5 the `v3AndBelow*` fields are not encoded, and no error is raised. Needed
  before the transactional producer. Confidence: high.
- **`latestVersionUnstable` is ignored.** `generator/Schema/Message.php` has
  no such field. `InitProducerId` gets the range 0-6, and version 6 is
  unstable. Low impact. Confidence: high.
- **The null marker of a nullable struct is wrong in both directions.**
  `generator/Generator.php:1278-1281` (write) and `1465-1474` (read). Kafka
  writes -1 for null and 1 for present. The generated code writes 0 for null
  and reads 0xFF as "present". Affects `ConsumerGroupHeartbeatResponse`,
  `ShareGroupHeartbeatResponse`, `DescribeTopicPartitionsResponse`,
  `DescribeTopicPartitionsRequest`, `StreamsGroupHeartbeatRequest`. The
  library does not send these yet. Needed for share groups. Confidence: high
  on the generated code; the marker values are from the auditor's memory of
  Kafka, check them against the Kafka source.
- Unverified: when a nullable string is absent in a version and has no
  default, the generator uses `null` (`Generator.php:1496`); Kafka uses `""`.

## 013 Topic lookup in metadata

- **A new topic is reported as unknown.** `src/Internal/Cluster.php:75-82`,
  `:453-461`; the same pattern in
  `src/Producer/Internal/Sender/BatchProducer.php:149-151`. A metadata
  request goes to a random broker. A broker that has not learned the topic
  yet answers `UNKNOWN_TOPIC_OR_PARTITION`. Every topic error is treated as
  "the topic does not exist", without a retry. Likely cause of the flaky
  test failure "Topic 'x' is unknown for cluster". Confidence: high on the
  code, medium-high on the link to the test.
- **Two refreshes for one topic race.** The answers are applied in arrival
  order. A late "unknown" answer deletes a good cache entry.
- **No coalescing.** `src/Internal/Cluster.php:84-87`, `:160-186`. N
  concurrent `produce()` calls on a new producer send 2N Metadata requests.
  `MetadataProvider` promises one refresh. Confidence: high.
- **Cached topics are never refreshed by the timer.** The timer asks only
  for `preloadTopics`. A producer never sees new partitions.
  `Metadata::$loadedAtMs` is written and never read. Confidence: high.
- Unverified: stale leader ids can point to a broker that `discover()`
  removed, which gives `UnknownBroker`.

## 014 Cancellation and the metadata timer in Cluster

- **The metadata timer throws inside an event loop callback.**
  `src/Internal/Cluster.php:109-114`. Any failure of `refreshMetadata()` is
  an uncaught exception that stops the loop. This matches the test failure
  "Uncaught AllBrokersFailed thrown in event loop callback". Confidence:
  high on the code.
- **The cluster has no closed state.** A `request()` after `close()` starts
  the timer again and connects through the seeds. A refresh that is in
  flight during `close()` opens a connection after the close or throws in
  the callback.
- **`doBrokerRequest()` catches every `Throwable`.**
  `src/Internal/Cluster.php:352-356`, `:372-376`, `:383-387`. A
  `CancelledException` moves the request to the next broker. The request is
  written to every broker, and the caller gets `AllBrokersFailed`.
  `AllBrokersFailed` is a `NetworkException`, so `Retry\Operation` retries a
  cancellation. Non-network errors (`NoCompatibleRequestVersion`,
  `MalformedResponse`) also go to all brokers. Confidence: high.
- **Group consumer effects of the same bug.** `close()` during
  `FindCoordinator` or a metadata refresh makes the next `consume()` throw
  `AllBrokersFailed`. A rebalance signal during `consumer->start()` kills
  the consumer. `Coordinator::request` drops the cached coordinator on a
  plain cancellation (`Coordinator.php:76-78`).
- **Shared work runs under the first caller's cancellation.**
  `src/Internal/Cluster/Broker.php:57-58` (connect),
  `src/Internal/Connection.php:335-344` (re-authentication),
  `src/Consumer/Internal/GroupConsumer/Coordinator.php:116-127` (coordinator
  discovery). One cancelled waiter fails all waiters. For re-authentication
  it also closes the connection. Confidence: high on the code.

## 015 Connection failures and leaks

- **The writer coroutine has no error handling.**
  `src/Internal/Connection.php:558-570`, `:155`. After a close the writer
  writes to a closed socket. The exception is uncaught in the event loop.
  Callers suspended in `$this->send->push()` are never resumed: `push()`
  takes no cancellation, and `complete()` does not release them. A candidate
  for the test that hung. Confidence: high on the mechanism.
- **A failed ApiVersions handshake leaks the socket.**
  `src/Internal/ConnectionFactory.php:71`: the handshake is outside the
  `try`. Each failed attempt leaks one socket and one fiber. Confidence:
  high.
- **`Broker::close()` ignores connections that are still opening.**
  `src/Internal/Cluster/Broker.php:68-81`, `:108-117`. The connection
  finishes later and is never closed. The `onClose` callback removes by
  kind, not by identity, so an old connection can remove a new one.
  Confidence: high for the first part, medium for the second.
- **A timed-out request leaves a dead connection in use.**
  `src/Internal/Connection.php:177-187`. A half-open connection is never
  closed. Every later request waits the full request timeout.
- **No frame size limit.** `src/Internal/Connection/ReadBuffer.php:24-27`
  trusts the 4-byte length, up to 4 GiB.
- **State is not checked again after a suspend.** After `refreshSession()`
  the `push()` on a completed queue throws a raw `\Error`, and the pending
  entry from line 152 is never removed.
- Unverified: if the connection is already closed when `onClose()` is
  registered (`Broker.php:108-112`), the closed connection stays in use
  forever. A `sessionLifetimeMs` of 0 on re-authentication
  (`Connection.php:388`) makes every request re-authenticate. An IPv6
  advertised host without brackets breaks `Endpoint::fromString`
  (`Cluster.php:397`, `:443`).

## 016 Producer shutdown

- **`close()` re-sends in-flight batches.**
  `src/Producer/Internal/Sender/PartitionWorker.php:56-64`,
  `src/Producer/Internal/Sender/BatchProducer.php:114-134`. `stop()` cancels
  and does not wait for the writes. `BatchProducer` catches `\Throwable`, so
  a cancellation is retried. Each attempt writes the batch to the socket
  before the cancelled await throws. Up to 10 copies in the log, and the
  user's future fails with `CancelledException`. Confidence: high.
- **`flush()` does not wait for released batches.**
  `src/Producer/Internal/TopicProducer.php:106-124`. Only open batches are
  awaited. This contradicts the doc of `Producer::flush()`.
- **No closed state.** `src/Producer/Internal/AsyncProducer.php:120-149,
  176`. A second `close()` throws `\Error`. `produce()` after `close()` for
  a new topic returns a future that never resolves, and the linger timer
  later throws inside the event loop. Confidence: high.
- **The producer never closes its `Cluster`.** `src/Client.php:58-59`. The
  sockets and the metadata timer stay alive after `close()`. Confidence:
  high.
- **A failed `InitProducerId` also throws `UnhandledFutureError`.**
  `src/Producer/Internal/IdempotencyCoordinator.php:81-91`. The first
  caller's cancellation is used for the shared request.
- **`BatchFull` in a bulk `produce()` loses the futures.**
  `src/Producer/Internal/AsyncProducer.php:83-88`. Records before the full
  one are queued, but the caller gets only the exception.
- Found in task 038: `AsyncProducer::close()` is idempotent now. The rest of
  "No closed state" is open.
- Found in task 014: "The producer never closes its `Cluster`" is confirmed.
  After `produceSync()` and `close()` the event loop still had three sockets
  and the metadata timer, and `EventLoop::run()` did not return in 15 s.
  `Cluster` has a closed state now, so the producer only has to call it.
- Found in task 013: a producer is closed while a `produce()` still waits for
  the metadata. The linger timer fires after the close, and the event loop
  gets "Values cannot be enqueued after calling complete" from
  `TopicProducer::openNew()`. The next tests of the same run fail too.
- Found in task 015: `push()` into a queue without a cancellation in
  `src/Producer/Internal/Sender/PartitionWorker.php` (`enqueue()`) and in
  `src/Producer/Internal/TopicProducer.php` (`release()`). The caller waits
  forever when nobody reads the queue. Not checked on a broker. Note: do not
  rely on `dispose()` after `complete()` to release such callers. It does
  nothing in `amphp/pipeline` 1.2.0 and 1.2.1, which `composer.json` allows.

## 017 Producer concurrency

- **A concurrent `produce()` loses a batch.**
  `src/Producer/Internal/TopicProducer.php:80-92`, push at `:246`. Fiber A
  releases a full batch and suspends in `push()`. Fiber B opens a new batch
  for the same partition. A resumes and replaces B's batch. B's record is
  never sent and its future never completes. Confidence: high on the logic.
- **One slow partition blocks all partitions.**
  `src/Producer/Internal/Sender.php:93-102`,
  `src/Producer/Internal/Sender/PartitionWorker.php:47-54, 91-92`. The
  router suspends in an unbuffered `push()`. No batch of any other partition
  is routed meanwhile. The doc comment of `Sender` says the opposite.
  Confidence: high.
- **`produceTimeout` is not enforced.**
  `src/Producer/Internal/Sender/BatchProducer.php:56-61, 172`. It is only
  sent to the broker. The request has no client timeout, because the worker
  cancellation is not null. Confidence: high.
- Unverified: `TopicProducer::release()` calls the metadata lookup (line
  231) before the batch is marked as released. A throw there happens inside
  the linger timer callback.

## 018 Idempotent producer after a failed batch

- **A failed batch leaves a gap in the sequence.**
  `src/Producer/Internal/TopicProducer.php:239-244`,
  `src/Producer/Internal/IdempotencyCoordinator.php:106-109`. Sequences are
  reserved at release and never rolled back. After a permanent failure the
  broker answers `OutOfOrderSequenceNumber` for every later batch of the
  partition. Confidence: high on the gap, medium on the broker reaction.
- **`DuplicateSequenceNumber` is treated as a failure.** The Java client
  treats it as success.
- **The sequence does not wrap at int32 max.**

## 019 Fetch errors

- **Partition errors are only logged.**
  `src/Consumer/Internal/Fetcher.php:221-230` (also 209-218, 87-96). No
  offset reset, no metadata refresh, no new leader, no wait. With
  `OffsetOutOfRange` the loop spins and `autoOffsetReset` is never applied.
  After a leader move a direct consumer never recovers. Confidence: high.
- **A dead fetch loop is not reported.**
  `src/Consumer/Internal/Fetcher.php:83-99`. `dispatch()` is outside the
  `try`. An `UnsupportedCodec`, `CorruptRecordBatch` or `UnexpectedEof` ends
  the loop silently, and `consume()` waits forever. In a group the member
  keeps its partitions and reads nothing. Confidence: high.
- **`ListOffsets` errors are a bare `\RuntimeException`, without a retry.**
  `src/Consumer/Internal/OffsetLister.php:127-132`.
- **`AutoOffsetReset::None` is sent as timestamp 0.**
  `src/Consumer/Internal/OffsetLister.php:97, 146`. Only for the direct
  consumer. Confidence: medium.
- Unverified: partitions are always sent in the same order, so a partition
  can starve. Fetch has no client timeout (`Fetcher.php:114`).
  `RecordBatch::decode` does not check the batch length.

## 020 Consumer lifecycle

- **`start()` is not exception-safe.**
  `src/Consumer/Internal/AssignmentConsumer.php:76-88`,
  `src/Consumer/Internal/DirectConsumer.php:47-60`. A cancelled or failed
  first `consume()` leaves the consumer broken: later calls throw
  `\LogicException('Consumer already running.')`. The docs promise an empty
  result on cancellation. `DirectConsumerTest::latestConsumesOnlyMessagesProducedAfterStart`
  uses a 0.2 s timeout, so this is a candidate for its flakiness.
  Confidence: high.
- **Batches are delivered after `close()`.**
  `src/Consumer/Internal/AssignmentConsumer.php:141, 244-249`. `complete()`
  keeps the buffered values. Confidence: high.
- **The busy flag is set before the generator starts.**
  `src/Consumer/Internal/AssignmentConsumer.php:112` against 253-255; the
  same in `GroupConsumer.php:90, 279-281`. An iterable that is dropped
  without iteration blocks every later `consume()` with `ConsumerBusy`.
- **Poll mode returns one batch.**
  `src/Consumer/Internal/AssignmentConsumer.php:240-250`. The buffer beyond
  the first batch is not returned. The direct consumer ignores
  `prefetchBufferSize` (line 82).
- **`CallableHandler` can wait for itself.**
  `src/Consumer/Internal/CallableHandler.php:63-67, 75-78`. `close()` from
  inside the handler never returns. The destructor cancels, so a
  subscription that is not stored stops at once. Confidence: medium.
- **A fatal error leaves the autocommit timer running.**
  `src/Consumer/Internal/GroupConsumer.php:72-90, 125-127, 199-207`.
  `close()` in the idle state does not close the `Cluster`.

## 021 Group close and rebalance

- **`close()` hangs when the buffer is full.**
  `src/Consumer/Internal/GroupConsumer/GroupSession.php:79`,
  `src/Consumer/Internal/GroupConsumer.php:138-139`. `$queue->push($batch)`
  cannot be cancelled. With more than `prefetchBufferSize` (4) unread
  batches, `close()` waits forever. Two auditors found this on their own.
  Confidence: high.
- **A rebalance is delayed the same way.** The session reacts to a rebalance
  signal only after the user takes the next batch. A slow handler gets the
  member removed from the group.
- **The buffer survives a rebalance.**
  `src/Consumer/Internal/GroupConsumer.php:62, 188-196`. Buffered batches
  are fetched again by the new session: duplicates on every rebalance.
  Batches of revoked partitions are still delivered. Stale entries can
  later commit an old offset. Confidence: high, medium for the last part.
- **`close()` is not bounded by `LeaveGroup`.**
  `src/Consumer/Internal/GroupConsumer.php:220-248`,
  `Coordinator.php:136-187`. The leave request has no timeout, and
  `findCoordinator()` has no attempt limit. Confidence: high.
- **Group requests have no request timeout.**
  `src/Internal/Cluster.php:119` as used from `Coordinator.php:61-65`. A
  lost response blocks the heartbeat forever. Confidence: high on the code.
- **A dead fetcher is not noticed by the session.** See 019.
- Found in task 015: the same note as in 016 about `dispose()` after
  `complete()` in `amphp/pipeline` 1.2.0 applies to the fix of the `push()`
  in `GroupSession`. Run the tests with the lowest dependencies
  (`make install-lowest`).

## 022 Group commits

- **Autocommit commits records that are not processed yet.**
  `src/Consumer/Internal/GroupConsumer.php:257-263`. A batch is marked
  before it is yielded. A crash in the middle of a batch loses the rest.
  Confidence: high.
- **`commitUncommitted()` hides every failure.**
  `src/Consumer/Internal/GroupConsumer.php:117-120, 284-296`. The public
  method shares the autocommit path, which only logs. Confidence: high.
- **Commits during a rebalance.** `commitRecords()` in the join window gets
  a raw `ProtocolError`, without a retry. It does not check ownership, so it
  can move the offset of a partition that another member owns now.
- Test race, not a library bug: `twoMembersEachOwnOnePartition` can read
  partition 0 twice when member A joins alone first.

## 009 Topic ids in offset commit

- `src/Consumer/Internal/GroupConsumer/OffsetCommitter.php:124` falls back
  to a nil topic id when the topic is not in the metadata. With a version
  that addresses topics by id the broker rejects the commit.

## 023 Group membership

- **Static membership cannot join.**
  `src/Consumer/Internal/GroupConsumer/JoinController.php:54-74, 90-104`.
  The member id from a successful `JoinGroup` response is never used. A
  static member then syncs with an empty id. No test uses
  `groupInstanceId`. Confidence: high on the code, medium-high on the broker
  behaviour.
- **A short coordinator outage kills the consumer.**
  `src/Consumer/Internal/GroupConsumer/Coordinator.php:56-104`. Five
  attempts take about 1.5 s. The session timeout is 45 s. Confidence:
  medium-high.
- Unverified: `JoinController::join` loops without a wait or a limit on
  `RebalanceInProgress`. `doJoin` never calls
  `ClassicGroupBalancer::memberMetadata()`, so a cooperative balancer cannot
  work. Offsets are resolved before the heartbeat starts
  (`GroupSession.php:52`).
- Difference from Java: range and round-robin do not sort static members by
  instance id first.

## 024 Compression codecs

- **Snappy cannot read batches of Java clients.**
  `src/Compressor/Snappy.php:43`. Java writes the xerial framing;
  `snappy_uncompress` accepts one raw block. Confidence: high on the format,
  not run.
- **LZ4 uses the block format.** `src/Compressor/Lz4.php:37, 43`. Kafka
  needs the LZ4 frame format. Confidence: medium-high, not run.
- **No size limit on decompression.**
  `src/Compressor/compression.php:33-42, 55-64`. A `false` result without a
  warning becomes a `TypeError`.
- The `php` image has no `snappy`, `lz4` or `zstd` extension. The only codec
  tests are `tests/Compressor/NoneTest.php` and `RegistryTest.php`.
- Unverified: zstd frames of Java producers have no content size; old
  versions of the extension reject them.

## 025 Send the client id

- **`Config::$clientId` is never sent.**
  `src/Internal/Connection/WriteBuffer.php:35`: `clientId: null`. Every
  request header has a null client id. Broker quotas, request logs and
  metrics by client id do not work. `clientId` is used nowhere else in
  `src/`. Confidence: high.

## 028 Compare the port of an endpoint

- **`Endpoint::matches()` compares only the host.**
  `src/Config/Endpoint.php:57-60`, used in `src/Internal/Cluster.php:416`. A
  broker that keeps its node id and host but changes its port is still
  called on the old port. Confidence: high on the code; a rare event.

## 029 Timestamp of LogAppendTime records

- **Wrong timestamp for `LogAppendTime` batches.**
  `src/Consumer/Internal/Protocol/RecordBatch.php:163` always uses
  `firstTimestamp + timestampDelta`. For a topic with
  `message.timestamp.type=LogAppendTime` every record must report the
  `maxTimestamp` of the batch. `Attributes::timestampType` is parsed and
  never read. Confidence: high.

## 030 Errors lost by Errors::for()

- **The top-level error of Metadata is lost.**
  `src/Protocol/Errors.php:138-161`; caller
  `src/Internal/Cluster.php:248-249`. `fromMetadata()` reads only topic and
  partition codes. A Metadata v13 response with a top-level error gives
  "Kafka request failed with 0 protocol error(s)". Confidence: high.
- **Responses that are not in the list report no errors.**
  `src/Protocol/Errors.php:65`: `default => []`. For example DeleteRecords,
  OffsetForLeaderEpoch, ConsumerGroupHeartbeat, DescribeTopicPartitions.
- `fromFetch()` reports an empty topic name for versions that carry only the
  topic id.

## 031 Library exceptions in place of generic ones

- `\LogicException` in `src/Consumer/Internal/AssignmentConsumer.php:77,
  109`.
- `\LogicException` in `src/Consumer/Internal/GroupConsumer.php:107`
  (`commitRecords()` before the first join).
- `\RuntimeException` in `src/Consumer/Internal/OffsetLister.php:129`.
- `\RuntimeException` in
  `src/Producer/Internal/Sender/PartitionWorker.php:50`.
- `\LogicException` in `src/Producer/Internal/TopicProducer.php:68`
  ("TopicProducer is dead") and in
  `src/Producer/Internal/AsyncProducer.php:161` ("No producer record topic
  and no default").
- `\RuntimeException` in
  `src/Producer/Internal/IdempotencyCoordinator.php:141` ("InitProducerId
  failed").
- The produce path in `BatchProducer` is covered by task 008.
- `AGENTS.md` does not allow generic exceptions to reach the user. Check
  every `throw new \` in `src/` outside of `src/Protocol/Generated/` when
  this task is planned.
- Found in tasks 008 and 013: done already: `OffsetLister`,
  `PartitionWorker`, `IdempotencyCoordinator` ("InitProducerId failed") and
  `BatchProducer`. Still open in the produce path:
  `src/Producer/Internal/AsyncProducer.php` ("No producer record topic and
  no default"), `src/Producer/Internal/TopicProducer.php` ("TopicProducer is
  dead", a user reaches it with `produce()` after `close()`), and
  `src/Producer/Internal/RecordBatch.php` (the codec does not match).

## 026 Write and read performance

- **A batch is encoded again on every attempt.**
  `src/Producer/Internal/Sender/BatchProducer.php:56-57, 163-186`.
- **Per-record overhead in the producer.**
  `src/Producer/Internal/AsyncProducer.php:164-174` builds loggers for each
  record. `recordOverhead()` runs twice. `TopicProducer::flush()` rebuilds
  the futures array with a spread (lines 114-120).
- **`Records::add()` rebuilds the array for every record.**
  `src/Consumer/Internal/Protocol/Records.php:44-50`.
- **Extra copies of each fetched batch.**
  `src/Consumer/Internal/Protocol/RecordBatch.php`: `read`, `peek` for the
  CRC, `read` again. `array_filter` runs on every batch
  (`Fetcher.php:289-292`).
- **Socket reads use 8192-byte chunks.**
  `src/Internal/Connection.php:430`,
  `src/Internal/Connection/ReadBuffer.php:21-31`. A 1 MiB response needs
  about 128 wake-ups, and the frame is copied twice.
  `WriteBuffer.php:29-45` copies the whole request to add 4 bytes.
- **Known tagged fields are always encoded.**
  `generator/Generator.php:911-963`. Kafka omits a tagged field that has its
  default value.

## 027 One Produce request per broker

- `src/Producer/Internal/Sender/BatchProducer.php:163-186` always builds a
  request with one topic and one partition. N partitions on one broker mean
  N requests per linger interval.

## 042 Connection and metadata leftovers

Found while tasks 013, 014 and 015 were implemented. No task covered them.

- **A slow but legal request looks like a dead connection.**
  `Connection::closeIfSilent()` in `src/Internal/Connection.php` closes a
  connection that owes an answer and is silent for `Config::$requestTimeout`.
  A Fetch waits up to `Consumer\Config::$maxWait`, and a JoinGroup waits up
  to `Consumer\GroupConfig::$rebalanceTimeout`. When the user sets one of
  them to the request timeout or more, a cancellation of such a request
  closes the connection and logs "connection does not answer". With the
  defaults (5 s and 10 s against 30 s) it does not happen. Found in task 015,
  from the code. Not reproduced.
- **`Cluster::$topicAnswers` only grows.** `src/Internal/Cluster.php` keeps
  the number of the last metadata answer for every topic name it ever saw,
  also for deleted topics. A client that works with many short-lived topics
  leaks memory slowly. Found in task 013.
- **`Metadata::$loadedAtMs` is written and never read.** Found in task 013.
- **Old connection removes a new one.** `Broker` removes a connection from
  its list by kind, not by identity, and clears the opening slot by kind.
  Task 015 could not reproduce it: a new connection of a kind is made only
  after the old one was removed. Left as it is.
- Unverified, moved from section 015: if the connection is already closed
  when `onClose()` is registered in `Broker`, the closed connection stays in
  use. A `sessionLifetimeMs` of 0 on re-authentication makes every request
  re-authenticate. An IPv6 advertised host without brackets breaks
  `Endpoint::fromString`.
- **A scenario of the test kit that ran out of time keeps running.**
  `tests/Testing/Setup.php` stops waiting for the closure and cleans up, but
  the closure stays in the event loop until the process ends. It can disturb
  the next tests of the run. Found in task 038.

## Checked and found correct

- Murmur2 matches the Java client. Record encoding, zigzag varints, null key
  and value, headers, negative timestamp deltas, CRC range.
- Sticky rotation of the partitioners. A record larger than `batchSize` is
  sent alone.
- Correlation id wrap. No double completion of a request.
- `Retry\Operation` attempt count and backoff.
- SCRAM and PLAIN exchange, session expiry.
- Fixed-width integers, compact lengths, nullable markers of strings, bytes
  and arrays, UUID byte order, tagged field order, request header versions.
- CRC32C.
- Field filling for JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
  FindCoordinator and OffsetFetch in every version.
- Committed offset is the last offset plus one, with the leader epoch.
- Range and round-robin match the Java assignors for uneven partition
  counts.
- Offsets after compaction, empty batches and control batches. Resume in the
  middle of a batch.
