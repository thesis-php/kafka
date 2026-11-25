# 005 Read committed consumer

Type: fixes

## Goal

A consumer with `IsolationLevel::ReadCommitted` never returns records of
aborted transactions. Today it returns them.

## Context

- `Consumer\Config` has `isolationLevel`. The consumer sends it in the `Fetch`
  request (`src/Consumer/Internal/Fetcher.php`). With `ReadCommitted` the
  broker returns records only up to the last stable offset, so records of
  open transactions do not arrive.
- Records of aborted transactions do arrive. The broker does not remove them.
  It returns a list of aborted transactions for each partition:
  `abortedTransactions` in the generated
  `FetchResponse\FetchableTopicResponse\PartitionData`. Each entry has a
  producer id and the first offset of the aborted transaction. The client
  must drop these records itself.
- A transaction ends in the log with a control batch. It holds a marker
  (commit or abort) for one producer id. Control batches are detected in
  `src/Consumer/Internal/Protocol/RecordBatch.php`: their records are skipped,
  and the marker inside is not read.
- A batch has the `isTransactional` attribute. Batches of a plain or an
  idempotent producer do not have it and are never dropped.
- The filtering algorithm is described in the Kafka protocol guide and in
  KIP-98: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98

## What to do

When the consumer reads with `ReadCommitted`, drop every transactional record
that belongs to an aborted transaction. Use the list of aborted transactions
from the fetch response and the control batches in the log to know where each
aborted transaction starts and ends.

The position of the consumer still moves past dropped records and past
control batches, so they are not fetched again.

With `ReadUncommitted` nothing changes.

## Decisions

- No public API changes. `IsolationLevel::ReadCommitted` starts to do what
  its name says.
- This is a bug fix: the option existed but did not filter.
- Filtering works for the direct consumer and for the group consumer.
- Committed offsets and the position include dropped records. An offset
  inside an aborted transaction is a valid place to continue from.
- Control batches are never returned to the user, with any isolation level.

## Tests

Every scenario runs against Kafka 3.9 and against Kafka 4.x. Records are
written with the transactional producer from task 003.

- Aborted records are hidden: one transaction is aborted, the next one is
  committed, both to one partition. A `ReadCommitted` consumer reads only the
  committed records. A `ReadUncommitted` consumer reads all of them.
- Mixed producers: a transactional producer aborts, and a plain producer
  writes to the same partition between its records. `ReadCommitted` returns
  all records of the plain producer and none of the aborted ones.
- Two transactional producers write to one partition at the same time. One
  commits, the other aborts. `ReadCommitted` returns only the records of the
  first one.
- Resume inside an aborted range: a group consumer commits an offset, the
  next records in the log belong to an aborted transaction followed by
  committed ones. A new consumer of the same group reads only the committed
  records and does not hang.
- A direct consumer and a group consumer both pass the first scenario.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass on both brokers.
- [ ] The existing consumer tests pass without changes.
- [ ] `README.md` describes `ReadCommitted` in the consumer section.
- [ ] `make check` passes.

## Out of scope

- Changes to the transactional producer.
- Exposing the last stable offset or consumer lag to the user.

## Depends on

003
