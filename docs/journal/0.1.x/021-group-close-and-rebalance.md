# 021 Group close and rebalance

Type: fixes

## Goal

`close()` of a group consumer always returns in bounded time, and a
rebalance does not deliver records twice or deliver records of partitions
that the member no longer owns.

## Context

- The findings are in `audit-findings.md`, section "021". Read it first.
- `src/Consumer/Internal/GroupConsumer/GroupSession.php` reads batches from
  the fetchers and puts them into a queue for the user. The queue holds
  `prefetchBufferSize` batches (default 4). When it is full, the session
  waits, and this wait cannot be cancelled.
- The same queue lives for the whole consumer, also across rebalances. See
  `src/Consumer/Internal/GroupConsumer.php`.
- The session reacts to a rebalance signal from the heartbeat only between
  two batches.
- `close()` leaves the group with `LeaveGroup`. That request has no timeout.
- Group requests (join, sync, heartbeat, offset fetch) run without a request
  timeout.
- A test of the group consumer once hung forever. The existing tests write
  too few batches to fill the buffer, so the cause is not proven.

## What to do

The wait for free space in the user queue stops when the consumer is closed
or when the group rebalances.

When a generation ends, the batches that the user has not taken yet are
dropped. They are fetched again from the committed offset by the member that
owns the partition after the rebalance.

`close()` gives the group a short time to accept the leave request. After
that time it closes anyway.

Every group request has a time limit, so a lost response cannot block the
heartbeat or the join forever.

## Decisions

- No public API changes. Time limits come from existing config values
  (`requestTimeout`, `rebalanceTimeout`, `sessionTimeout`). If a new value
  is needed, stop and ask the user.
- `close()` never throws because the leave request failed. The failure is
  logged.
- After a rebalance the user never gets a batch of a partition that was
  revoked in that rebalance.
- Records that were delivered but not committed can be delivered again
  after a rebalance. This is the at-least-once contract and does not change.
- Test first: write the failing test before the fix. If a finding cannot be
  reproduced through the public API, do not fix it blindly. Report it.

## Tests

- Close with a full buffer: a topic has many more batches than
  `prefetchBufferSize` (for example 50 batches made with a small
  `batchSize`). The consumer reads one batch and calls `close()`. `close()`
  returns within a few seconds.
- Close from the handler: the same with `Client::consume()` and a handler
  that closes the session after the first batch.
- No revoked records: member A reads slowly from a topic with two
  partitions and a full buffer. Member B joins. After the rebalance A gets
  no batch from the partition that moved to B.
- No duplicates from the buffer: with autocommit off and a commit after
  every batch, two members process a topic with a rebalance in the middle.
  Every record is processed once.
- Slow handler: a handler that takes longer than the heartbeat interval,
  with a rebalance during that time. The member rejoins without being
  removed from the group.
- `close()` when the coordinator does not answer returns in bounded time. If
  the test setup cannot stop a coordinator, report it and describe how it
  was checked by hand.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass, or the report explains which one
      could not be automated.
- [ ] The first two tests fail (hang) without the fix. The task report shows
      it.
- [ ] Every new test has its own time limit, so a hang fails the test and
      does not block the test run.
- [ ] `make test` passes five times in a row.
- [ ] `make check` passes.

## Out of scope

- What autocommit commits and how commit errors are reported (task 022).
- Static membership and coordinator outage tolerance (task 023).
- Fetch errors and a dead fetch loop (task 019).

## Depends on

014
