# 014 Cancellation and the metadata timer in Cluster

Type: fixes

## Goal

A cancelled request ends as a cancellation, and a failed background refresh
never stops the event loop. Today a cancellation is turned into
`AllBrokersFailed` after the request was sent to every broker, and an error
in the metadata timer is an uncaught exception that kills the application.

## Context

- The findings are in `audit-findings.md`, section "014". Read it first.
- `src/Internal/Cluster.php` tries the brokers one by one and catches every
  `Throwable`, also `CancelledException` and errors that are not about the
  network.
- The periodic metadata refresh runs in an event loop timer. An exception
  from a timer callback is not caught by anyone. A test run once ended with
  "Uncaught AllBrokersFailed thrown in event loop callback".
- `Cluster` has no closed state: a request after `close()` starts the timer
  again.
- Three places share one piece of work between callers and run it under the
  cancellation of the first caller: connecting to a broker
  (`src/Internal/Cluster/Broker.php`), re-authentication
  (`src/Internal/Connection.php`) and coordinator discovery
  (`src/Consumer/Internal/GroupConsumer/Coordinator.php`).

## What to do

A cancellation stops the request at once and reaches the caller as
`CancelledException`. The request is not sent to other brokers. Only network
errors make the cluster try the next broker.

A failure of the periodic refresh is logged and does not leave the timer
callback. The next tick tries again.

After `close()` the cluster does nothing more: no timer, no new
connections. A request on a closed cluster fails with a library exception.

Work that is shared between callers does not depend on the cancellation of
one of them. Each caller can stop waiting on its own.

## Decisions

- No public API changes.
- A request on a closed cluster throws a library exception. Use an existing
  one if it fits (for example `Exception\ConnectionClosed`).
- The periodic refresh failure is logged at warning level.
- A cancelled waiter of a shared re-authentication does not close the
  connection. Only a failure of the exchange itself does.
- Test first: write the failing test before the fix. If a finding cannot be
  reproduced through the public API, do not fix it blindly. Report it.

## Tests

- Cancel is cancel: a request through the public API (for example
  `Client::request()` or `consume()` with a cancellation) that is cancelled
  ends with `CancelledException` or with the documented empty result, not
  with `AllBrokersFailed`.
- Close during startup: a group consumer is closed while its first
  `consume()` is still joining. `close()` returns, and nothing throws
  `AllBrokersFailed`.
- Timer failure: a client with a short metadata max age points to a broker
  that becomes unreachable after the first request. The process keeps
  running, and the failure is in the log. If the test setup cannot make a
  broker unreachable, report it and describe how it was checked by hand.
- After `close()` of a producer and of a consumer, the event loop has no
  work left from them: the script ends without waiting for the metadata max
  age.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass, or the report explains which one
      could not be automated.
- [ ] No test run shows "Uncaught ... thrown in event loop callback" from
      `Cluster`.
- [ ] `make check` passes.

## Out of scope

- The producer does not close its `Cluster` (task 016). This task only makes
  a closed cluster and its timer safe.
- Connection writer failures and leaks (task 015).
- Retry of retriable topic errors (task 013).

## Depends on

nothing
