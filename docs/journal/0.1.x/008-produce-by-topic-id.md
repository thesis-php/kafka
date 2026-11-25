# 008 Produce by topic id

Type: fixes

## Goal

The producer works with brokers that support Produce version 13, where a
topic is addressed by its id and not by its name. Today every produce to such
a broker fails. This fixes https://github.com/thesis-php/kafka/issues/1.

## Context

- From version 13 the Produce request and response carry the topic id and no
  topic name (KIP-516). See `schemas/messages/ProduceRequest.json` and
  `schemas/messages/ProduceResponse.json`.
- The client picks the highest version that both sides support, so with a new
  broker it uses version 13.
- `src/Producer/Internal/Sender/BatchProducer.php` builds the request with a
  nil topic id. The broker answers with `UNKNOWN_TOPIC_ID`.
- The same class finds the topic in the response by name. In version 13 the
  name is empty, so the answer is not found. The user sees "Broker omitted
  partition ... from response" and not the real error.
- The topic id is in the metadata: `Internal\Cluster\Metadata\TopicMetadata`
  has `topicId`. The consumer already works by topic id. See
  `src/Consumer/Internal/Fetcher.php`.
- Old brokers do not know topic ids. For them the metadata has a nil id, and
  the Produce version is below 13, so the name is used.
- `ErrorCode::UnknownTopicID` is retriable. The producer refreshes the
  metadata between attempts.
- Produce errors reach the user as `\RuntimeException`. `AGENTS.md` does not
  allow that. `Exception\ProtocolError` exists and carries the Kafka errors.
- The Kafka 3.9 test cluster does not support Produce version 13. The bug can
  be reproduced only on the Kafka 4.x broker from task 002.
- KIP-516: https://cwiki.apache.org/confluence/display/KAFKA/KIP-516

## What to do

Build the Produce request with the real topic id from the metadata, together
with the topic name. Each protocol version then encodes the field it needs.

Find the topic in the response by name when the response has names, and by
topic id when it has ids.

When the broker reports an error for the partition, the user gets that error,
not a message about a missing partition.

Replace the generic exceptions in the produce path with library exceptions
that carry the Kafka error code.

## Decisions

- The full fix. The client keeps Produce version 13; the version is not
  capped.
- No public API changes, except new or reused exception types.
- Versions below 13 behave as before.
- If the topic id changed because the topic was deleted and created again,
  the next attempt uses the new id after the metadata refresh. No special
  code path for this case is needed beyond the existing retry.
- A broker error for a produced batch fails the record futures with a library
  exception that has the Kafka error code, the topic and the partition.
  `\RuntimeException` is not thrown to the user from the produce path.
- A response that really has no entry for the partition is still an error,
  with a library exception.
- The nil topic id in `OffsetCommitter` is not part of this task. It is task
  009.

## Tests

The first three scenarios run against the Kafka 4.x broker and check in the
test that the negotiated Produce version is 13 or higher. If the broker from
task 002 does not support version 13, stop and report it.

- Produce and consume: an idempotent producer with manual flush sends 100
  records to an existing topic, as in the issue. All futures succeed. A
  consumer reads the same 100 records.
- A plain producer (not idempotent) sends records to a topic with several
  partitions. Each result has the right partition and offset.
- Topic created again: produce to a topic, delete it, create a topic with the
  same name, produce again with the same producer. The second produce
  succeeds.
- Broker error is visible: a record that the broker rejects (for example a
  record larger than the topic allows) fails its future with a library
  exception that names the Kafka error. Runs against both brokers.
- The existing producer tests pass on Kafka 3.9 without changes.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass.
- [ ] The produce path throws no `\RuntimeException` to the user.
- [ ] The existing tests pass.
- [ ] `make check` passes.

## Out of scope

- Topic ids in other requests (task 009).
- Capping protocol versions in the generator.
- Changes to the generated protocol classes.

## Depends on

002
