# 013 Topic lookup in metadata

Type: fixes

## Goal

A topic that exists is always found. Today a producer or a consumer can fail
with "Topic 'x' is unknown for cluster" right after the topic was created,
and the tests fail from time to time because of it. The metadata cache also
stays fresh and is not loaded many times at once.

## Context

- The findings are in `audit-findings.md`, section "013". Read it first.
- A metadata request goes to a random broker. Brokers learn about a new topic
  at different moments. A broker that does not know the topic yet answers
  with a retriable error. `src/Internal/Cluster.php` treats every topic
  error as "the topic does not exist".
- Every producer and every consumer has its own `Internal\Cluster`. The test
  helper `waitUntilTopicVisible()` in `tests/TestCase.php` asks through the
  client's cluster, so it does not prove that other brokers know the topic.
- `Internal\MetadataProvider` promises that concurrent calls for one topic
  make one refresh. The code does not do that.
- The periodic refresh asks only for the preloaded topics, not for the
  topics in the cache.

## What to do

When a topic lookup gets a retriable topic error, try again for a short time
before the topic is reported as unknown. Only an error that cannot be
retried, or the end of that time, ends the lookup.

A late "unknown" answer must not remove a good entry from the cache.

Concurrent lookups of the same topic share one metadata request.

The periodic refresh also refreshes the topics that are in the cache, so a
producer sees new partitions of a topic.

## Decisions

- No public API changes. If a new config value is needed for the lookup
  time, stop and ask the user.
- A topic that really does not exist is still reported with
  `Exception\UnknownTopic`, after the retries.
- A topic error that is not retriable (for example an authorization error)
  is reported at once, with a library exception that carries the Kafka
  error code. It is not reported as "unknown topic".
- Test first: write the failing test before the fix. If a finding cannot be
  reproduced through the public API, do not fix it blindly. Report it.
- The test helper `waitUntilTopicVisible()` stays. The fix must work without
  it too.

## Tests

- New topic, new producer: create a topic and produce to it at once with a
  new producer, without `waitUntilTopicVisible()`. Repeat many times in one
  test (for example 30 topics). Every produce succeeds.
- New topic, new consumer: the same with a direct consumer and a group
  consumer.
- A topic that does not exist: produce fails with `UnknownTopic`, and the
  time until the failure is bounded.
- New partitions: a producer writes to a topic, the test adds partitions to
  the topic, and after the metadata max age the producer writes to a new
  partition.
- Many concurrent `produce()` calls on a new producer succeed. Check the
  number of metadata requests if the public API or the logs allow it;
  otherwise report that it was checked by hand.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass.
- [ ] The first three tests fail without the fix. The task report shows it.
- [ ] `make test` passes five times in a row without the "unknown for
      cluster" failure.
- [ ] `make check` passes.

## Out of scope

- Cancellation handling and the metadata timer errors (task 014).
- Retry of produce after a leader change (task 011).

## Depends on

nothing
