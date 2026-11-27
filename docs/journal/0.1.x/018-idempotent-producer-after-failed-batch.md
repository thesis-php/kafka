# 018 Idempotent producer after a failed batch

Type: fixes

## Goal

An idempotent producer keeps working after a batch failed for good. The
later batches of the same partition are delivered, and the other partitions
do not notice the failure. Today one failed batch can break the partition
for the whole life of the producer.

## Context

- The findings are in `audit-findings.md`, section "018". Read it first.
  They were checked against the code when this task was planned. The note
  "Planned in task 018" at the end of the section says what is still true
  and what moved to task 044.
- With `Producer\Config::$idempotent` the producer asks the broker for a
  producer id and an epoch once
  (`src/Producer/Internal/IdempotencyCoordinator.php`, the result is
  `Producer\Internal\Idempotency`). Each batch carries the producer id, the
  epoch and a base sequence. The broker accepts a batch of a partition only
  when its base sequence is the next one for this producer id and epoch.
- `IdempotencyCoordinator` keeps the next sequence of each partition.
  `TopicProducer::openNew()` (`src/Producer/Internal/TopicProducer.php`)
  reads it and puts it into the `Protocol\RecordBatch`
  (`src/Producer/Internal/Protocol/RecordBatch.php`) together with the
  producer id and the epoch. All three are fixed when the batch is opened.
  `TopicProducer::release()` moves the sequence of the partition forward by
  the number of records. Nothing moves it back.
- A released batch waits in the queue of its partition
  (`src/Producer/Internal/Sender/PartitionWorker.php`) and is sent by
  `Sender\BatchProducer`. An idempotent producer has one request in flight
  per partition: `Producer\Config` does not allow another value of
  `maxInflightProduceRequest`.
- A batch fails for good when the broker answers with an error that cannot
  be retried, when the attempts are used up, or when the last attempt runs
  out of time (`Exception\ProduceTimedOut`, task 017). The sequence of the
  partition has already moved forward. So the next batch has a base
  sequence that the broker does not expect. The expected answer is
  `OutOfOrderSequenceNumber` for this batch and for every later one. Nobody
  has seen this on a broker yet.
- Some failures do not tell whether the broker wrote the batch: a timeout
  or a lost connection on the last attempt. So the client cannot simply
  use the old sequence again: when the broker did write the batch, the
  broker would take the next batch for a copy of it.
- The Java client solves this with an epoch bump
  ([KIP-360](https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent+and+transactional+producer)).
  An idempotent producer without transactions raises its epoch by one on
  its own, without a request, and starts the sequence of the partition at
  0. A broker accepts a higher epoch of a known producer id when the
  sequence is 0.
- `ErrorCode::DuplicateSequenceNumber` (`src/Protocol/ErrorCode.php`) is
  not retriable, so `BatchProducer::send()` fails the batch. The Java
  client takes it as a success. A broker answers a copy of one of the last
  five batches with a success and the old offset. With one request in
  flight a retry is always the newest batch, so the error code should not
  appear.
- The tests of the idempotent producer:
  `idempotentProducerPublishesSuccessfully` in `tests/ProducerTest.php`,
  `concurrentIdempotentProduceToOnePartitionLosesNoRecord` in
  `tests/ProducerConcurrencyTest.php`. `tests/ProduceTimeoutTest.php` shows
  how to make an attempt run out of time.

## What to do

First show the problem on a broker: a test where a batch of an idempotent
producer fails for good and the next record of the same partition must be
delivered.

Then make the producer recover. After a batch of a partition failed for
good, the producer raises its epoch and the partition starts its sequence
at 0 again. Every batch of this partition that is sent after the failure
uses the new epoch and the new sequences, also a batch that was already
open or already waited in the queue when the failure happened. So the
numbers of a batch can no longer be fixed when the batch is opened.

A batch keeps its epoch and its sequence over all its attempts. This is
what lets the broker find a copy.

The logs that print the epoch or the base sequence must print the values
that are really sent.

## Decisions

- No `@api` item changes. The exception of the failed batch is the same as
  today.
- The rule is the same for every batch that fails for good, whatever the
  reason: an error of the broker, used up attempts, `ProduceTimedOut`. The
  producer does not try to find out whether the broker wrote the batch.
- The epoch is raised by one, without a request to the broker. The
  producer id stays.
- The partition of the failed batch starts at sequence 0 with the new
  epoch.
- Only the failed batch fails. The batches of the partition behind it are
  delivered, in the order of their release. The log of the partition has
  no record of the failed batch between them, unless the broker did write
  it.
- Another partition is not disturbed: its batch in flight keeps its
  numbers. A partition may move to the newest epoch later, but only when
  it has no batch in flight, and then its sequence starts at 0.
- A batch that fails because of the cancellation of `close()` needs no
  recovery: the producer is closed.
- When the broker answers `OutOfOrderSequenceNumber`, `UnknownProducerID`
  or `InvalidProducerEpoch`, the batch fails with `Exception\ProtocolError`
  as today. It is not sent again with new numbers. The rule above then
  repairs the partition for the next batches.
- `DuplicateSequenceNumber`: try to get it from a broker in a test. When a
  test gets it, the batch is a success: the futures complete with a
  `Producer\Produced` whose `offset` and `timestamp` are null. When no
  test can get it, do not change the code and say so in the report.
- An epoch above 32767 and a sequence above 2147483647 are not handled
  here. See "Out of scope".
- Test first: write the failing test before the fix.

## Tests

The first scenario runs on Kafka 3.9 and on Kafka 4.x, with the data
provider `Kafka::versions`: it depends on how the broker treats a new
epoch. The others run where their broker setup is.

- Failed batch, then success: a topic with a small `max.message.bytes`
  and an idempotent producer. A small record is delivered. A record that
  is too large for the topic fails with `ProtocolError` and
  `MessageTooLarge`. The next small record is delivered and gets the
  offset after the first one. A consumer reads the two small records.
  Before the fix this test must fail; note in the report with which error.
- Several failures: the same setup, two large records fail one after the
  other, with small records between and after them. Every small record is
  delivered.
- Batches behind the failed one: on `Kafka::PROXIED_BROKER` with a stalled
  proxy. The batch of the large record is in flight, and batches of small
  records wait behind it. After the proxy drops the connections, the large
  batch fails, the small ones are delivered in order.
- Another partition: a topic with two partitions. Records go to both. A
  batch of partition 0 fails for good. Records that are produced to
  partition 1 before and after the failure are delivered with offsets
  without a gap.
- Timeout, the broker did not get the batch: every attempt is silent
  (`Proxy::silence()`), the future fails with `ProduceTimedOut`. Then the
  proxy works again and the next record of the partition is delivered.
  The kit needs a way to end the silence.
- Timeout, the broker did write the batch: the answers come too late
  (`Proxy::delayAnswers()`), every attempt runs out of time, the future
  fails with `ProduceTimedOut`. Then the delay ends and the next record is
  delivered. A consumer reads the first record, the record of the failed
  future exactly once, and the next record.
- A retried batch is not a copy: an idempotent producer, the first attempt
  runs out of time after the broker wrote the batch, the second attempt
  succeeds. The future has the offset of the first write, and a consumer
  reads the record exactly once.
- `DuplicateSequenceNumber`: see "Decisions".
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] After a batch failed for good, the later batches of the partition
      are delivered.
- [ ] Batches that waited behind the failed batch are delivered in order.
- [ ] A failure in one partition does not fail or delay batches of another
      partition.
- [ ] A batch has the same epoch and sequence on every attempt.
- [ ] The report says which error the broker gave before the fix, and what
      happened with `DuplicateSequenceNumber`.
- [ ] `README.md`, section "Idempotent producer", says what happens after
      a failed record.
- [ ] `make check` passes.

## Out of scope

- The sequence after 2147483647 and the epoch after 32767 (task 044). No
  integration test can reach them.
- More than one request in flight for an idempotent producer.
- To send a batch again with new numbers after `OutOfOrderSequenceNumber`
  or `UnknownProducerID`, as the Java client does.
- The transactional producer (task 003). It bumps the epoch with a request
  to the transaction coordinator.
- A new producer id after an error of the broker that names the producer
  id.

## Depends on

017
