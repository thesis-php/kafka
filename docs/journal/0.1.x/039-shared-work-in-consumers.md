# 039 Shared work in consumers and cancellation

Type: fixes

## Goal

A cancelled caller in a consumer only stops its own wait. It does not fail
other callers and does not throw away state that is still valid. Task 014
fixed this for connections. Two places in the consumers still have the old
pattern, and task 014 could not reproduce them.

## Context

- Task 014 made a shared connect (`src/Internal/Cluster/Broker.php`) and a
  shared re-authentication (`src/Internal/Connection.php`) independent of the
  cancellation of one caller. Read these two as the example of the fix.
- `src/Consumer/Internal/GroupConsumer/Coordinator.php`:
  - `request()` catches every `\Throwable` from the cluster request and calls
    `invalidate()`. A `CancelledException` is also a `\Throwable`. So a plain
    cancellation drops the cached coordinator, and the next request sends
    `FindCoordinator` again.
  - `discover()` starts `findCoordinator()` as a shared future under the
    cancellation of the first caller, and every waiter clears the future in
    its `finally`.
- `src/Consumer/Internal/DirectConsumer.php`: `consume()` starts `setup()` as
  a shared future under the cancellation of the first caller.
- A second `consume()` on a consumer that is already consuming throws
  `ConsumerBusy`. See `DirectConsumerTest::concurrentConsumeThrowsConsumerBusy`.
  So it is not clear that two callers can wait for the same `setup()` at all.
- The coordinator logs `group coordinator discovered` and
  `group coordinator invalidated`. A test can see the log: the test kit gives
  a logger to its clients with `Setup::logger()`. See
  `tests/MetadataRefreshTest.php`.
- Task 014 found no way through the public API to bring two callers into
  `Coordinator::discover()` at the same time.

## What to do

For each of the three places, first try to show the problem with a test
through the public API. The log is a valid way to observe it.

When the problem can be shown, fix it as task 014 did: a cancelled caller
stops waiting, the shared work goes on for the others, and a cancellation
does not invalidate the coordinator.

When two callers cannot reach the shared work through the public API, the
sharing is dead code. Remove it and keep the simple path.

## Decisions

- No public API changes.
- Test first: write the failing test before the fix.
- A cancellation of a coordinator request does not drop the cached
  coordinator. Only a failure of the request does.
- A place that cannot be reached by two callers through the public API is
  simplified: the shared future is removed. It is not fixed without a test.
  The existing tests must pass after that.
- The report says for each of the three places what was tried, and whether
  the place was fixed or simplified.
- A test may check log records to observe the coordinator. It checks the
  message and the count, not the full context.

## Tests

- Cancel keeps the coordinator: a group consumer has found its coordinator. A
  call that goes to the coordinator is cancelled. The next call works, and
  the log has no `group coordinator invalidated` record and no second
  `group coordinator discovered` record.
- Shared discovery, if it can be reached: two calls wait for the coordinator
  at the same time, and the first one is cancelled. The second call gets its
  result.
- Shared setup of a direct consumer, if it can be reached: the same scenario
  for two callers of `consume()`.
- The existing consumer tests pass without changes.

## Acceptance criteria

- [ ] A cancelled coordinator request does not cause a new `FindCoordinator`.
- [ ] Each of the three places is fixed with a test or simplified, and the
      report says which.
- [ ] The existing tests pass.
- [ ] `make check` passes.

## Out of scope

- `IdempotencyCoordinator` shares its work under the cancellation of the
  first caller. This is task 016.
- Close and rebalance of a group consumer (task 021).
- Coordinator outage and attempt limits (task 023).

## Depends on

014
