# 028 Compare the port of an endpoint

Type: fixes

## Goal

Two endpoints match only when the host and the port are the same. Today
only the host is compared, so a broker that keeps its node id and host but
gets a new port is still called on the old port.

## Context

- The finding is in `audit-findings.md`, section "028". It was checked
  against the code when this task was planned and is still true.
- `Thesis\Kafka\Config\Endpoint` (`src/Config/Endpoint.php`) is an `@api`
  class with the public properties `$host` and `$port`.
  `Endpoint::matches()` returns `$this->host === $endpoint->host`.
- `matches()` has one caller: `Internal\Cluster::discover()`
  (`src/Internal/Cluster.php`). For each broker of a metadata answer it
  keeps the existing `Cluster\Broker` when the endpoint matches, and closes
  and replaces it when it does not.
- No broker of the test cluster changes its port while it runs, so an
  integration test cannot show the wrong call. `Endpoint` is public API,
  so its test calls `matches()` directly.

## What to do

Make `Endpoint::matches()` compare the host and the port. The cluster then
replaces a broker whose port has changed, with the code it has already.

## Decisions

- `Endpoint::matches()` returns true only when the host and the port are
  both equal. The signature does not change. This is a change of the
  behaviour of an `@api` method, agreed with the developer.
- The host is compared as today: as a string, without a lookup and without
  a change of the letter case.
- Add a doc comment to `matches()` that says what it compares.
- No other code changes: `Cluster::discover()` keeps its logic.

## Tests

- Same host and port: `matches()` returns true.
- Same host, other port: `matches()` returns false.
- Other host, same port: `matches()` returns false.
- The other existing tests pass without changes. A client still keeps its
  connections after a metadata refresh that reports the same brokers.

## Acceptance criteria

- [ ] `Endpoint::matches()` compares the host and the port.
- [ ] `make check` passes.

## Out of scope

- A new bootstrap when all known broker addresses have changed (deferred
  to 0.2.x, KIP-899).
- The generic exceptions of `Endpoint::fromString()` (task 031).
- Other leftovers of the connections and the metadata (task 042).

## Depends on

nothing
