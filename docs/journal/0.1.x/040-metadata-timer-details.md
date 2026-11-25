# 040 Metadata timer: interval and log

Type: fixes

## Goal

The periodic metadata refresh runs at the interval the user set, and a failed
refresh is logged once. Today an interval below one second becomes zero, and
one failure makes two log records.

## Context

- `src/Internal/Cluster.php` starts the timer in `request()` with
  `$this->config->metadataMaxAge->toSeconds()`. `TimeSpan::toSeconds()` has
  precision 0 by default and returns an integer. So 0.4 s becomes 0, and
  1.4 s becomes 1.
- `Config::$metadataMaxAge` is public API. Its default is
  `Config::DEFAULT_METADATA_MAX_AGE_SECS`.
- Other places in the same class already use `toSeconds(3)`, for example for
  the request timeout.
- Task 014 made the timer callback catch a failure and log
  `periodic metadata refresh failed` at warning level.
- `Cluster::doMetadataRequest()` also logs every failure as
  `metadata refresh failed` at error level and throws it again. So a failed
  periodic refresh is logged twice. An explicit metadata request that fails
  or is cancelled is logged as an error too, and the caller also gets the
  exception.
- `tests/MetadataRefreshTest.php` shows how a test makes the refresh fail: a
  proxy from the test kit (`Kafka::proxy()`) and a logger (`Setup::logger()`).

## What to do

Give the timer the interval with millisecond precision.

Remove the error log from the metadata request. A failure of an explicit
request reaches the caller as an exception. A failure of the periodic refresh
has no caller, so the timer logs it, once, at warning level.

## Decisions

- No public API changes.
- The interval has millisecond precision.
- A failed periodic refresh makes one log record, at warning level.
- A failed or cancelled explicit metadata request is not logged by the
  cluster. The caller gets the exception.
- A `metadataMaxAge` of zero or less is not handled in this task.
- Test first: write the failing test before the fix.

## Tests

- Interval below one second: a client has a `metadataMaxAge` below one
  second, and its broker becomes unreachable. The log gets a new
  `periodic metadata refresh failed` record about once per interval. It is
  not a tight loop, and it is not once per second.
- One record per failure: in the same scenario each failed refresh makes one
  warning record, and the log has no `metadata refresh failed` error record.
- The existing tests pass without changes.

## Acceptance criteria

- [ ] The periodic refresh runs at an interval below one second when the
      user sets one.
- [ ] A failed periodic refresh is logged once.
- [ ] The existing tests pass.
- [ ] `make check` passes.

## Out of scope

- Validation of `Config` values.
- Routing of requests to the controller (task 041).
- The producer does not close its `Cluster` (task 016).

## Depends on

014
