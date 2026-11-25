# 004 Transaction errors and fencing

Type: feat

## Goal

The transactional producer handles errors the way Kafka requires. The user
always knows which of three things to do after an error: nothing (the producer
retried), abort the transaction, or stop using the producer.

## Context

- Task 003 adds the transactional producer with the normal flow only.
- Kafka error codes are in `Protocol\ErrorCode`. `ErrorCode::retriable()`
  already marks the codes that can be retried. `Protocol\Errors` collects the
  errors of a response. `Protocol\Error\TransactionError` exists.
- Transaction errors fall into three groups:
  - retriable: the coordinator moved or is loading, another operation of the
    same transaction is still running (`ConcurrentTransactions`). The request
    is sent again.
  - abortable: the transaction cannot be committed, but the producer is fine
    after an abort. A failed produce inside a transaction is the usual case.
    Kafka 4.x also reports `TransactionAbortable`.
  - fatal: the producer cannot be used any more. `ProducerFenced`,
    `InvalidProducerEpoch`, `TransactionalIDAuthorizationFailed` and others.
- Fencing: when a second producer starts with the same transactional id, the
  broker raises the epoch. The first producer gets a fatal error on its next
  transactional request or produce.
- KIP-98: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98
- KIP-360: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360
- KIP-890: https://cwiki.apache.org/confluence/display/KAFKA/KIP-890

## What to do

Sort every error that a transactional request or a produce in a transaction
can return into one of the three groups, and make the producer act on it.

Retriable errors are retried inside the producer, within the limits the
producer already has for retries and timeouts.

After an abortable error the transaction is marked as failed. Until the user
aborts it, `produce()` and `commitTransaction()` throw. After
`abortTransaction()` the producer works again.

After a fatal error the producer is dead. Every later call throws the same
error, except `close()`.

## Decisions

- The three groups are visible to the user through exception types. One type
  says "abort the transaction and continue", another says "this producer is
  dead". A fenced producer has its own exception type, because the user
  handles it differently: another instance took over.
- The exceptions carry the transactional id and the Kafka error code as
  public readonly properties.
- `commitTransaction()` on a failed transaction throws the abortable error. It
  does not abort by itself.
- `abortTransaction()` on a failed transaction succeeds and makes the
  producer usable again.
- `close()` never throws because of a failed or fenced transaction.
- The futures of records that were produced in a transaction that later
  aborts keep their own result. An acknowledged record stays acknowledged; it
  is just never visible to `ReadCommitted` consumers.
- Fencing is logged at warning level, once.

## Tests

Every scenario runs against Kafka 3.9 and against Kafka 4.x.

- Fencing on commit: producer A begins and produces. Producer B is created
  with the same transactional id and begins. A's commit throws the fenced
  exception. B commits without an error.
- Fencing on produce: the same, but A produces again after B started. The
  produce or the following commit throws the fenced exception.
- A dead producer stays dead: after the fenced exception, `beginTransaction()`
  throws the same exception. `close()` does not throw.
- Abortable error: a record that the broker rejects inside a transaction (for
  example a record larger than the topic allows) fails its future. Commit
  throws the abortable exception. Abort succeeds. The next transaction on the
  same producer commits.
- Commit after an abortable error does not make any record of that
  transaction visible to a `ReadCommitted` consumer.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass on both brokers.
- [ ] No transaction error reaches the user as `\RuntimeException` or
      `\LogicException`.
- [ ] The "Transactions" section of `README.md` explains the three groups of
      errors and what to do in each case.
- [ ] `make check` passes.

## Out of scope

- Recovery from a fatal error inside the same producer object (epoch bump on
  the client, KIP-360). The user creates a new producer.
- Errors of consumer offsets in a transaction (task 006).

## Depends on

003
