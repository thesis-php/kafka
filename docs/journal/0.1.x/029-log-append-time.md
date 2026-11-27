# 029 Timestamp of LogAppendTime records

Type: fixes

## Goal

A record of a topic with `message.timestamp.type=LogAppendTime` reports the
time when the broker wrote it, and the user can see which kind of time a
record has. Today such a record reports the time that the producer sent,
which the broker has replaced.

## Context

- The finding is in `audit-findings.md`, section "029". It was checked
  against the code when this task was planned and is still true.
- `Consumer\Internal\Protocol\RecordBatch::decode()`
  (`src/Consumer/Internal/Protocol/RecordBatch.php`) reads the header of a
  batch, with `firstTimestamp`, `maxTimestamp` and the attributes. For
  each record it sets
  `timestamp: $records->firstTimestamp + $record->timestampDelta`.
- `Internal\Protocol\Record\Attributes`
  (`src/Internal/Protocol/Record/Attributes.php`) parses the timestamp
  type of the batch into the internal enum
  `Internal\Protocol\Record\TimestampType` (`CreateTime`,
  `LogAppendTime`). Nothing reads it.
- With `LogAppendTime` the broker sets this attribute and writes its own
  time into `maxTimestamp` of the batch. It does not rewrite the time of
  each record. So every record of such a batch has the time
  `maxTimestamp`.
- `Thesis\Kafka\Consumer\Record` (`src/Consumer/Record.php`) is an `@api`
  class with a public constructor and the property `$timestamp`, in
  milliseconds.
- `Producer\Record` (`src/Producer/Record.php`) has an optional
  `$timestamp`, so a test can send a record with an old time.
- The test kit creates a topic with configs:
  `$kafka->topic(configs: ['message.timestamp.type' => 'LogAppendTime'])`.
- Other clients show the type of the time to the user: the Java client
  with `ConsumerRecord::timestampType()`, and Go clients (franz-go,
  confluent-kafka-go) with a timestamp type of the record.
- Kafka documentation, topic config `message.timestamp.type`:
  https://kafka.apache.org/documentation/#topicconfigs_message.timestamp.type

## What to do

Use the timestamp type of the batch when the time of a record is set, and
give this type to the user with each record.

## Decisions

- New `@api` enum `Thesis\Kafka\Consumer\TimestampType` with the cases
  `CreateTime` and `LogAppendTime`.
- `Consumer\Record` gets a new public property `$timestampType` of this
  enum. It is the last parameter of the constructor and has the default
  `TimestampType::CreateTime`, so code that creates a `Consumer\Record`
  today still works. This change of an `@api` constructor is agreed with
  the developer.
- A record of a batch with `LogAppendTime` has `$timestamp` equal to
  `maxTimestamp` of the batch and `$timestampType` `LogAppendTime`. Every
  record of one batch has the same time.
- A record of a batch with `CreateTime` has the time as today
  (`firstTimestamp` plus the delta of the record) and `$timestampType`
  `CreateTime`.
- The doc of `Consumer\Record::$timestamp` says which time it is for each
  type.
- Whether the internal enum stays or the public one replaces it is the
  choice of the implementer.
- Test first: write the failing test before the fix.

## Tests

- Log append time: a topic with `message.timestamp.type=LogAppendTime`. A
  producer sends two records in one batch with an old timestamp, for
  example one hour ago. A consumer reads them. Both have the type
  `LogAppendTime`, the same timestamp, and this timestamp is not older
  than the start of the test.
- Create time: a topic with the default config. A producer sends two
  records with two different old timestamps. A consumer reads them. Each
  has the type `CreateTime` and its own timestamp.
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] A record of a `LogAppendTime` topic reports the time of the broker.
- [ ] `Consumer\Record::$timestampType` tells the type of the time.
- [ ] `README.md`, section "Consumer", names the new property where it
      describes a record. If no place describes a record, add one
      sentence about the timestamp and its type.
- [ ] `make check` passes.

## Out of scope

- The message formats before v2 (magic 0 and 1). The client does not read
  them (`Exception\UnsupportedMessageFormat`).
- A producer that asks for the type of the time of a topic.
- A search of an offset by a timestamp (task 034).

## Depends on

nothing
