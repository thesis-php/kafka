# 015 Connection failures and leaks

Type: fixes

## Goal

A connection that fails or closes never leaves a caller waiting forever,
never throws inside the event loop, and never leaks a socket.

## Context

- The findings are in `audit-findings.md`, section "015". Read it first.
- `src/Internal/Connection.php` has a reader coroutine and a writer
  coroutine. Requests go through a send queue with a small buffer. The
  writer has no error handling.
- `amphp/pipeline`: `Queue::complete()` keeps the buffered values and does
  not resume callers that are suspended in `push()`.
- `src/Internal/ConnectionFactory.php` does the ApiVersions handshake and
  the SASL authentication for a new connection.
- `src/Internal/Cluster/Broker.php` keeps the open and the opening
  connections of one broker.
- A test of the group consumer once hung forever. The writer failure is one
  candidate for it. This is not proven.

## What to do

When the writer fails, the connection closes itself, and every caller gets
an error: the ones that wait for a response and the ones that wait to put a
request into the send queue.

A connection that fails during its handshake is closed.

A broker that is closed also closes the connections that were still opening.
The end of an old connection does not remove a newer one.

A request that timed out on a connection that does not answer does not
leave that connection in use for the next requests.

A frame that announces an unreasonable size is rejected.

## Decisions

- No public API changes. If a frame size limit needs a config value, use a
  fixed internal limit in this task and report it.
- Errors reach the caller as library exceptions (`ConnectionClosed`,
  `MalformedResponse` or others that exist). No raw `\Error` from a
  completed queue.
- Test first: write the failing test before the fix. If a finding cannot be
  reproduced through the public API, do not fix it blindly. Report it.
- A test may start its own TCP server inside the test process to play a
  peer that accepts connections and then stalls, closes, or sends garbage.
  This is not a mock of the broker protocol: it checks only how the client
  reacts to a broken peer.

## Tests

- Peer closes: a client sends requests to a peer that closes the connection
  after it accepts. Every request fails with a library exception in bounded
  time. Nothing is thrown inside the event loop.
- Many callers: more requests than the send buffer holds wait on one
  connection, then the connection closes. All of them fail; none hangs.
- Silent peer: a peer accepts the connection and never answers. The request
  fails after the request timeout, and after the client is closed no socket
  and no event loop work is left.
- Garbage peer: a peer answers with a frame length of 2 GiB. The request
  fails with a library exception and memory does not grow.
- Close during connect: a consumer is closed while it still connects. No
  connection is left open.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass, or the report explains which one
      could not be automated.
- [ ] The tests fail without the fix. The task report shows it.
- [ ] `make check` passes.

## Out of scope

- Cancellation and the metadata timer (task 014).
- Socket read and write performance (task 026).
- TLS.

## Depends on

nothing
