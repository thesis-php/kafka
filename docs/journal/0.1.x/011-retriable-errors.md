# 011 Retriable errors and leader change

Type: fixes

## Goal

The client retries the same errors as Kafka does, and a producer survives a
lost connection or a missing leader without a burst of useless attempts. An
error code that the client does not know never ends in a `ValueError`.

## Context

- The findings are in `audit-findings.md`, section "011". Read it first.
  Tasks 008 and 013 did a part of it; the section says which part.
- `src/Protocol/ErrorCode.php` is `@api`. `ErrorCode::retriable()` says
  whether an error can be retried. It is used by
  `src/Internal/Retry/Operation.php`, `src/Internal/Cluster.php` and
  `src/Producer/Internal/Sender/BatchProducer.php`.
- Compared with Kafka, the list has no `ReplicaNotAvailable`,
  `NotCoordinator`, `PreferredLeaderNotAvailable` and `InconsistentTopicID`.
  It has `BrokerNotAvailable`, which Kafka does not retry.
- The source of truth is `Errors.java` of the Kafka Java client: an error is
  retriable when its exception class extends `RetriableException`.
  https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
- `BatchProducer` sends a batch up to `Producer\Config::$produceAttempts`
  times (10 by default). After a retriable error code it waits
  (`Retry\backoff()`) and refreshes the metadata of the topic. Task 013 added
  this.
- The `catch (\Throwable)` of the same loop does none of it. It retries every
  exception at once, without a wait and without a metadata refresh. This
  includes a cancellation, and exceptions that a retry cannot fix, for
  example `Exception\UnknownPartition`.
- `ErrorCode::from()` throws `ValueError` for a code that the enum does not
  have. It is still used for codes from a broker in
  `src/Consumer/Internal/Fetcher.php`, `src/Internal/Cluster.php`,
  `src/Internal/Connection.php` (three places) and
  `src/Producer/Internal/IdempotencyCoordinator.php`. `BatchProducer` and
  `Protocol\Errors` already use the safe form: an unknown code becomes
  `ErrorCode::UnknownServerError`.
- Network failures extend `Exception\NetworkException`.
  `Retry\isNetwork()` in `src/Internal/Retry/Operation.php` checks it.
- `kafka-proxied-standalone` and `Kafka::proxy()` of the test kit put every
  connection of a client behind a proxy. See the kit notes in `AGENTS.md`.

## What to do

Make the list of `ErrorCode::retriable()` equal to the retriable errors of
Kafka. Check the whole list against `Errors.java`, not only the five codes
named above.

Make the exception path of the produce loop work like the error code path.
A failure that a new attempt can fix is retried after a wait and a metadata
refresh. Any other exception fails the batch on the first attempt. A
cancellation ends the batch at once.

Replace `ErrorCode::from()` for codes that come from a broker with the safe
form, in all the places listed in "Context".

## Decisions

- `ErrorCode::retriable()` follows Kafka exactly. `BrokerNotAvailable` is
  no longer retriable. This changes the result of an `@api` method. The
  signature does not change.
- When Kafka versions disagree about an error, the newest `Errors.java`
  wins. The report lists every code that was added or removed.
- In the produce loop these exceptions are retried: every
  `Exception\NetworkException`, `Exception\UnknownBroker` and
  `Exception\NoAvailablePartitionLeader`. Before the next attempt the
  producer waits with the same backoff as after a retriable error code and
  refreshes the metadata of the topic.
- A failed metadata refresh between two attempts does not fail the batch.
  The next attempt runs, and it counts as an attempt.
- Every other exception fails the batch on the first attempt, with that
  exception.
- A cancellation is never retried. The batch fails with the
  `CancelledException` at once.
- The number of attempts stays `Producer\Config::$produceAttempts`. No new
  config.
- An error code from a broker that the enum does not have is
  `ErrorCode::UnknownServerError`. It is not retriable.
- No other public API changes.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

- Retriable list: for every error code the result of `retriable()` is equal
  to a fixed list taken from `Errors.java`. `ErrorCode` is `@api`, so the
  test calls it directly.
- Lost connection during produce: a producer works behind a proxy on
  `kafka-proxied-standalone`. Its connections are dropped, and new
  connections work. A record produced after that is delivered, and the log
  shows a wait between the attempts, not a burst. Extend the proxy of the
  kit in the smallest way if it cannot drop connections and accept new ones.
- Broker gone for good: the same producer, the proxy is disconnected. The
  produce fails with a network exception after `produceAttempts` attempts.
  The time between the first and the last attempt shows the backoff. Use a
  small `produceAttempts` to keep the test short.
- No retry for a wrong partition: a record goes to a partition that the
  topic does not have. The produce fails with `Exception\UnknownPartition`,
  and the log has no `produce failed, retrying` record.
- Unknown error code, if a test peer can show it: a response with an error
  code that the enum does not have ends in a library exception or in an
  `UnknownServerError`, not in a `ValueError`. Use a peer like
  `Testing\BrokenPeer` only if it stays small. If it does not, say so in the
  report.
- The existing tests pass without changes.

## Acceptance criteria

- [ ] `ErrorCode::retriable()` is equal to the retriable errors of Kafka,
      and the report lists the differences that were found.
- [ ] A producer delivers a record after its connections were dropped.
- [ ] A network failure is retried with a wait and a metadata refresh.
- [ ] An exception that a retry cannot fix fails the batch on the first
      attempt.
- [ ] No `ErrorCode::from()` is left for a code that comes from a broker.
- [ ] The existing tests pass.
- [ ] `make check` passes.

## Out of scope

- Retries in the fetch path and its error handling (task 019).
- Producer shutdown and what `close()` does with batches that wait
  (task 016).
- The idempotent producer after a failed batch: sequence numbers and
  duplicates on retry (task 018).
- Coordinator outage and attempt limits of the group consumer (task 023).
- A total time limit of a produce (delivery timeout).

## Depends on

013
