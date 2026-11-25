# 006 Consumer offsets in a transaction

Type: feat

## Goal

A transactional producer can commit consumer group offsets as a part of its
transaction. The offsets and the produced records are then committed or
aborted together. This is the step that makes consume-transform-produce
exactly-once.

## Context

- Task 003 adds the transactional producer, task 004 its error handling.
- The group consumer commits offsets on its own today, with `OffsetCommit`.
  Start reading at `src/Consumer/Internal/GroupConsumer.php` and
  `src/Consumer/Internal/GroupConsumer/OffsetCommitter.php`. The public
  methods are `ConsumerSession::commitRecords()` and `commitUncommitted()`.
- In a transaction the flow is different. The producer tells the transaction
  coordinator that a group takes part (`AddOffsetsToTxnRequest`), then sends
  the offsets to the group coordinator (`TxnOffsetCommitRequest`). The offsets
  become visible only when the transaction commits.
- `TxnOffsetCommitRequest` carries the member id, the generation id and the
  group instance id of the consumer (KIP-447). With them the broker rejects
  offsets from a consumer that is no longer the owner of the partitions.
- With the transaction protocol v2 (Kafka 4.x) the client does not send
  `AddOffsetsToTxn`; the broker adds the group itself.
- KIP-447: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447

## What to do

Give the transactional producer a way to add the offsets of consumed records
to the open transaction. The producer needs to know which group consumer the
records came from, because the commit must carry the current membership of
that consumer.

The committed offset for a partition is the offset of the last given record
plus one, as in `commitRecords()`.

If the consumer lost its partitions or its generation changed, the broker
rejects the offsets. This is an abortable error of the transaction, as
defined in task 004.

## Decisions

- Public API: one new method on `TransactionalProducer` that takes the
  consumer and the records, for example
  `commitOffsets(Consumer $consumer, Consumer\Record|array $records): void`.
  The implementer may adjust the name and the parameter order to match the
  code around, and must report the final signature.
- The method works only inside an open transaction. Outside of one it throws.
- The method works only with a group consumer. With a direct consumer it
  throws `Exception\InvalidConfiguration`.
- The method can be called several times in one transaction. For one
  partition the highest offset wins.
- Offsets sent this way are not committed again by `commitUncommitted()` or
  by autocommit after the transaction commits.
- After an abort the consumer is not moved back. The user decides what to do
  with the records it already has. Task 007 builds the safe flow on top.
- A rejected offset commit because of a rebalance is an abortable error, not
  a fatal one.

## Tests

Every scenario runs against Kafka 3.9 and against Kafka 4.x.

- Commit: a group consumer reads records, the producer writes results and
  adds the offsets, then commits. A new consumer of the same group starts
  after the committed records. A `ReadCommitted` consumer of the output topic
  reads the results.
- Abort: the same, but the transaction aborts. A new consumer of the same
  group reads the input records again. The output has no visible results.
- Outside of a transaction the method throws.
- With a direct consumer the method throws `InvalidConfiguration`.
- Stale member: the consumer loses its partitions to a second member of the
  group before the offsets are added. The transaction fails with an abortable
  error, and nothing is committed.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass on both brokers.
- [ ] The "Transactions" section of `README.md` shows the method.
- [ ] The task report gives the final signature of the new method.
- [ ] `make check` passes.

## Out of scope

- The session that handles rebalances for the user (task 007).
- Committing offsets of a group without a consumer object (by group id and
  raw offsets).

## Depends on

004, 005
