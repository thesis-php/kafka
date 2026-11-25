# 007 Transact session

Type: feat

## Goal

One object for the consume-transform-produce loop with exactly-once
semantics: it reads committed records from a group, writes results in a
transaction, and commits the consumer offsets in the same transaction. It
keeps the loop correct when the group rebalances. This is what franz-go calls
`GroupTransactSession`.

## Context

- Task 003: transactional producer. Task 004: its errors. Task 005:
  `ReadCommitted` filtering. Task 006: consumer offsets in a transaction.
- The problem this task solves: between consuming a record and committing the
  transaction, the group can rebalance and give the partition to another
  member. That member starts from the last committed offset and processes the
  same records. If the first member then commits its transaction, the results
  are written twice.
- The group consumer knows when its assignment changes. Start reading at
  `src/Consumer/Internal/GroupConsumer.php`, where the old and the new
  assignment are compared.
- The broker rejects a transactional offset commit from a member with an old
  generation (KIP-447). The session must not rely on this alone: a rebalance
  can finish after the offsets were accepted and before the transaction ends.
- franz-go `GroupTransactSession`:
  https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#GroupTransactSession
- KIP-447: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447

## What to do

Add a session that owns one group consumer and one transactional producer.
The user consumes records through it, opens a transaction, produces results
through it, and ends the transaction with a commit or an abort.

On commit the session adds the offsets of all records it returned since the
last ended transaction, then commits. If the consumer lost any partition
since those records were returned, the session aborts instead and tells the
user that nothing was committed.

After an abort, by the user or because of a rebalance, the session moves the
consumer back to the last committed offsets of the partitions it still owns.
The records are then consumed again, by this member or by the new owner.

## Decisions

- Public API:
  - `Client::createTransactSession(string|array $topics, string
    $transactionalId, Consumer\Config $consumer, Producer\Config $producer =
    new Producer\Config()): TransactSession`.
  - `Thesis\Kafka\TransactSession` with:
    - `consume(...)`: the same parameters and result as `Consumer::consume()`;
    - `begin(): void`;
    - `produce(...)` and `produceSync(...)`: the same as in `Producer`;
    - `commit(): bool` — `true` if the transaction and the offsets were
      committed, `false` if the session had to abort because of a rebalance;
    - `abort(): void`;
    - `close(): void`.
- The consumer config must have a group, `IsolationLevel::ReadCommitted`, and
  autocommit off. If not, the factory throws `Exception\InvalidConfiguration`.
  Nothing is changed silently.
- The producer config follows the rules of task 003.
- `commit()` returns `false` only for a rebalance. Other errors are thrown,
  with the exception types of task 004.
- `produce()` outside of `begin()` ... `commit()`/`abort()` throws.
- `close()` aborts an open transaction, then closes the producer and the
  consumer.
- The session does not expose `commitRecords()` or `commitUncommitted()`.
  Offsets are committed only by `commit()`.
- A handler-style API on top of the session (like `Client::consume()`) is not
  part of this task.
- `README.md` gets a section for the session and `examples/` gets a runnable
  script.

## Tests

Every scenario runs against Kafka 3.9 and against Kafka 4.x.

- Exactly once, one member: N input records are consumed, each produces one
  output record, transactions are committed in small groups. The output topic
  has exactly N records for a `ReadCommitted` consumer, and a new member of
  the group has nothing left to read.
- Abort replays: a transaction is aborted. The same input records are
  returned again by `consume()`. After they are processed and committed, the
  output has each result once.
- Rebalance during a transaction: member A consumes and begins. Member B
  joins the same group and gets a part of the partitions. A's `commit()`
  returns `false`. After both members process everything, the output has each
  result exactly once.
- A wrong consumer config (no group, `ReadUncommitted`, autocommit on) makes
  the factory throw `InvalidConfiguration`. One case for each.
- `close()` with an open transaction leaves no visible output and no
  committed offsets.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass on both brokers.
- [ ] The rebalance scenario passes ten times in a row.
- [ ] `README.md` has a section for the session and `examples/` has a script
      that runs.
- [ ] `make check` passes.

## Out of scope

- A handler-style API on top of the session.
- The cooperative rebalance protocol and the new consumer group protocol
  (KIP-848).
- Share groups.

## Depends on

006
