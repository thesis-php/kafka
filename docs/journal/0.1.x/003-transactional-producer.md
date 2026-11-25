# 003 Transactional producer

Type: feat

## Goal

A producer can write records in a transaction: open it, produce, then commit
or abort. Committed records become visible to `ReadCommitted` consumers all at
once. This is the base for exactly-once processing.

## Context

- The idempotent producer already gets a producer id and an epoch from the
  broker and keeps a sequence for each partition. Start reading at
  `src/Producer/Internal/IdempotencyCoordinator.php` and
  `src/Producer/Internal/AsyncProducer.php`.
- The record batch already carries the producer id, the epoch and the
  attributes. `Internal\Protocol\Record\Attributes` has the `isTransactional`
  flag. It is never set today.
- `src/Consumer/Internal/GroupConsumer/Coordinator.php` shows how to find a
  coordinator and how to retry requests to it. The transaction coordinator is
  found with the same `FindCoordinator` request and key type 1.
- The requests are generated: `InitProducerIdRequest`,
  `AddPartitionsToTxnRequest`, `EndTxnRequest`. Read them and their schemas in
  `schemas/messages/` before use. `AddPartitionsToTxnRequest` has different
  fields for versions up to 3 and for version 4 and later.
- Two versions of the transaction protocol exist:
  - v1 (Kafka 3.9): the client sends `AddPartitionsToTxn` before the first
    record for a partition in a transaction.
  - v2 (Kafka 4.x, KIP-890): the broker adds partitions itself, and the
    `EndTxn` response returns a new producer epoch. The client must use this
    epoch for the next transaction.
- A `ReadCommitted` consumer gets records only up to the last stable offset.
  So records of an open transaction are not returned to it. The consumer does
  not filter aborted records yet (task 005).
- Kafka documentation: https://kafka.apache.org/documentation/#semantics
- KIP-98: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98
- KIP-890: https://cwiki.apache.org/confluence/display/KAFKA/KIP-890

## What to do

Add a transactional producer. It is created with a transactional id. On the
first use it finds the transaction coordinator and gets its producer id and
epoch for this transactional id. A producer with the same transactional id
that started earlier is fenced by the broker at this moment.

Between `beginTransaction()` and `commitTransaction()` or
`abortTransaction()` every produced batch is marked as transactional. Commit
and abort first wait until all records of the transaction are sent and
acknowledged, then end the transaction on the coordinator.

The producer works with both versions of the transaction protocol. It uses
what the broker supports.

This task covers the normal flow. Error handling beyond "the request failed,
throw the error" is task 004.

## Decisions

- Public API:
  - `Thesis\Kafka\TransactionalProducer extends Producer` with
    `beginTransaction(): void`, `commitTransaction(): void`,
    `abortTransaction(): void`.
  - `Client::createTransactionalProducer(string $transactionalId,
    Producer\Config $config = new Producer\Config(), ?TimeSpan
    $transactionTimeout = null): TransactionalProducer`. The transactional id
    is a non-empty string. The default timeout is 60 seconds.
  - The plain `Producer` interface and `Producer\Config` do not change.
- A transactional producer is always idempotent, also when the config has
  `idempotent: false`. The config must have `Acks::All` and
  `maxInflightProduceRequest: 1`. If not, the factory throws
  `Exception\InvalidConfiguration`.
- `produce()` and `produceSync()` outside of a transaction throw. They do not
  open a transaction.
- `beginTransaction()` when a transaction is open throws.
  `commitTransaction()` and `abortTransaction()` without an open transaction
  throw.
- `close()` with an open transaction aborts it, then closes the producer.
- `flush()` inside a transaction sends the buffered records. It does not end
  the transaction.
- Both protocol versions are supported. The version is chosen from what the
  broker reports, not from a config option.
- New exceptions extend `Thesis\Kafka\Exception` and live in
  `src/Exception/`.
- `README.md` gets a "Transactions" section and `examples/` gets a runnable
  script.

## Tests

Every scenario runs against Kafka 3.9 and against Kafka 4.x.

- Commit: begin, produce records to two partitions, commit. A `ReadCommitted`
  consumer reads all records.
- Open transaction is not visible: begin, produce, flush. A `ReadCommitted`
  consumer gets nothing. After commit it reads the records.
- Abort, then continue: begin, produce, abort; begin, produce, commit. A
  `ReadUncommitted` consumer reads the records of both transactions. The
  second transaction commits without an error.
- Several transactions in a row on one producer, to different topics. All
  commit. On Kafka 4.x this proves that the new epoch from `EndTxn` is used.
- `produce()` outside of a transaction throws.
- `beginTransaction()` twice throws. `commitTransaction()` without begin
  throws.
- `close()` with an open transaction: a `ReadCommitted` consumer never reads
  its records, and a new producer with the same transactional id can begin
  and commit at once.
- The factory throws `InvalidConfiguration` for a config without `Acks::All`.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass on both brokers.
- [ ] The plain producer behaves as before; its tests pass without changes.
- [ ] `README.md` has a "Transactions" section and `examples/` has a script
      that runs.
- [ ] `make check` passes.

## Out of scope

- Fencing, abortable and fatal errors, retries of transaction requests (task
  004).
- Filtering of aborted records in the consumer (task 005).
- Consumer offsets in a transaction (task 006).
- Two-phase commit (KIP-939).

## Depends on

002
