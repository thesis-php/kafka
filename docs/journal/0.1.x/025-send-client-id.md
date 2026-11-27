# 025 Send the client id

Type: fixes

## Goal

Every request of the client carries `Config::$clientId` in its header.
Today the header always has a null client id, so broker quotas, request
logs and metrics by client id do not work, and the setting does nothing.

## Context

- The finding is in `audit-findings.md`, section "025". It was checked
  against the code when this task was planned and is still true.
- `Thesis\Kafka\Config` (`src/Config.php`) has the public property
  `$clientId` (`non-empty-string`, with a default). Nothing in `src/`
  reads it.
- `Internal\Connection\WriteBuffer::write()`
  (`src/Internal/Connection/WriteBuffer.php`) builds the
  `RequestHeader` of every request with `clientId: null`. It is the only
  place that writes a request header.
- The broker shows the client id of a group member: `DescribeGroupsResponse`
  has `clientId` in
  `Generated\DescribeGroupsResponse\DescribedGroup\DescribedGroupMember`.
  The test kit has `groupMembers($groupId)`, which sends DescribeGroups and
  returns the member ids only.
- The kit creates clients with `createClient(...)`. It has no parameter for
  the client id.
- Kafka protocol guide, "Request header":
  https://kafka.apache.org/protocol#protocol_messages

## What to do

Put the client id of the config into the header of every request, on
every connection of the client.

## Decisions

- Every request has the client id: also ApiVersions, the SASL requests and
  the requests of every kind of connection.
- The value is `Config::$clientId`, unchanged. It is never null, because
  the property is a `non-empty-string`.
- No `@api` signature changes.
- `README.md` does not name `clientId` today. Add one sentence about it
  where the README shows how a client is created: what it is and that the
  broker sees it.
- Test first: write the failing test before the fix.

## Tests

- Client id of a group member: a client with its own client id creates a
  group consumer and reads a record. DescribeGroups for the group shows a
  member with this client id.
- Default client id: the same with the default config. The member has the
  default client id of `Config`, not an empty string.
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] The broker sees `Config::$clientId` of a client.
- [ ] No request header is written with a null client id.
- [ ] `README.md` names `clientId`.
- [ ] `make check` passes.

## Out of scope

- The rack id of a consumer (deferred to 0.2.x, fetch from the closest
  replica).
- Client quotas and throttling (task 033).
- Client telemetry (deferred to 0.2.x).

## Depends on

nothing
