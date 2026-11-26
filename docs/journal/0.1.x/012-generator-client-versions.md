# 012 Generator: client versions and null structs

Type: fixes

## Goal

The client offers to a broker only the request versions that a client may
use. The generated code writes and reads a nullable struct with the same null
marker as Kafka. The transactional producer needs the first, and requests of
Kafka 4 with nullable structs need the second.

## Context

- The findings are in `audit-findings.md`, section "012". Read it first.
  They were checked against the code and against Kafka trunk when this task
  was planned. All of them are true.
- `generator/Generator.php` builds `src/Protocol/Generated/`. `generate()`
  takes the version range of an API key from `validVersions` of the request
  schema and gives it to `Schema\ApiKey`. The result is
  `Generated\ApiKey::clientVersionRange()`.
  `src/Internal/NegotiatedVersions.php` negotiates this range with the range
  of the broker and uses the highest common version.
- `AddPartitionsToTxn` has `validVersions` 0-5. A client may use only 0-3:
  versions 4 and 5 are for brokers. The schema says it only in a comment
  (`schemas/messages/AddPartitionsToTxnRequest.json`). The Java client has
  it in code: `LAST_CLIENT_VERSION = 3` in
  https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/AddPartitionsToTxnRequest.java
  A Kafka 4 broker supports version 5, so the client would choose it. At
  version 5 the `v3AndBelow*` fields of the request are not encoded, and no
  error is raised. No other schema has such a comment today.
- A request schema can have `"latestVersionUnstable": true`. Then a broker
  does not offer the last version unless `unstable.api.versions.enable` is
  set, and the Java client does not use it. `generator/Schema/Message.php`
  does not read this field. Today only
  `schemas/messages/InitProducerIdRequest.json` has `true`, so
  `ApiKey::InitProducerId` has the range 0-6.
- A nullable struct that is not a tagged field has one marker byte before
  it. Kafka writes -1 for null and 1 for a value, and reads "null" when the
  byte is negative. See `MessageDataGenerator.java`:
  https://github.com/apache/kafka/blob/trunk/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java
  (`_writable.writeByte((byte) -1)`, `_writable.writeByte((byte) 1)`,
  `_readable.readByte() < 0`).
- The generator uses `writeBool` and `readBool` for this byte
  (`emitStructWrite()` and `emitStructRead()` in `generator/Generator.php`).
  It writes 0 for null. A broker reads 0 as "a value follows". It reads the
  -1 of a broker as `true` and then decodes a struct that is not there.
- The generated classes with such a field today:
  `ConsumerGroupHeartbeatResponse` (`assignment`),
  `ShareGroupHeartbeatResponse`, `DescribeTopicPartitionsRequest`
  (`cursor`), `DescribeTopicPartitionsResponse`,
  `StreamsGroupHeartbeatRequest`, and `StreamsGroupDescribeResponse`
  (`DescribedGroup`, `DescribedGroup\Member`). The library does not send
  them itself. A user can send them with `Client::request()`.
- Kafka has another null marker for a nullable struct in a tagged field. No
  schema has such a field today.
- The generator has its own test, `tests/Generator/GeneratorTest.php`. It
  gives schemas to the generator and compares the generated code.
- The Kafka 4.x test broker is `kafka-4-standalone:9092`
  (`Kafka::KAFKA_4_SEEDS` in the test kit).

## What to do

Teach the generator which versions of a request are for brokers only, and
keep them out of the client version range. The source is a small list inside
the generator, because the schemas do not have this information in a form
that a program can read.

Make the generator read `latestVersionUnstable` and keep an unstable last
version out of the client version range.

Make the generated code use the null marker of Kafka for a nullable struct,
when it writes and when it reads.

Run `make generate` and commit the generator and the generated files
together.

## Decisions

- The last client version of an API key comes from a list in the generator:
  API key name to last client version. Today it has one entry:
  `AddPartitionsToTxn`, version 3. The generator does not parse schema
  comments.
- The list has a comment that says where the numbers come from (the Java
  client) and that it must be checked after `make sync-schemas`.
- The generator fails with a clear message when an entry of the list names
  an API key that no schema has, or a version outside `validVersions` of
  that schema. So an old entry cannot stay unnoticed.
- When a request schema has `latestVersionUnstable: true`, the client
  version range ends one version before the last one.
- Only the range of `Generated\ApiKey::clientVersionRange()` changes:
  `AddPartitionsToTxn` becomes 0-3 and `InitProducerId` becomes 0-5. The
  request and response classes still encode and decode every version of the
  schema.
- `Generated\ApiKey` is `@api`. The results of `clientVersionRange()` change
  for these two keys. No signature changes.
- The null marker of a nullable struct: write -1 for null and 1 for a value.
  On read, a negative byte means null, and any other byte means that a
  struct follows. This is what Kafka does.
- A nullable struct in a tagged field is not a part of this task, because no
  schema has one. If the generator meets one, it must fail with a clear
  message. It must not generate a wrong marker.
- A nullable field that is absent in a version and has no default in the
  schema stays `null` after decode. Kafka gives an empty string, an empty
  array or an empty struct here. We keep `null` on purpose. Do not change it.
- No hand-written public API changes.
- Test first: write the failing test before the fix.

## Tests

- Generator, broker-only versions: a request schema with `validVersions`
  0-5 and an entry of the list with version 3 gives the range 0-3 in the
  generated `ApiKey`.
- Generator, bad entry of the list: an entry for an unknown API key, and an
  entry with a version outside `validVersions`, make the generator fail.
- Generator, unstable version: a request schema with `validVersions` 0-6
  and `latestVersionUnstable: true` gives the range 0-5. The same schema
  with `false` gives 0-6.
- Generator, null marker: a schema with a nullable struct gives code that
  writes -1 or 1 and reads a negative byte as null. Cover a struct that is
  nullable in all versions and one that is nullable only in some versions.
- Generator, tagged nullable struct: the generator fails.
- Real schemas: `ApiKey::AddPartitionsToTxn->clientVersionRange()` is 0-3
  and `ApiKey::InitProducerId->clientVersionRange()` is 0-5. `ApiKey` is
  `@api`, so the test calls it directly.
- Kafka 4.x, the client chooses a client version:
  `$kafka->negotiatedVersion(ApiKey::AddPartitionsToTxn)` is 3 and
  `$kafka->negotiatedVersion(ApiKey::InitProducerId)` is 5 at most.
- Kafka 4.x, null struct in a request: `DescribeTopicPartitionsRequest`
  with `cursor: null` for a topic of the test. The response has the
  partitions of the topic and no error. Before the fix the broker cannot
  read the request.
- Kafka 4.x, null struct in a response: `ConsumerGroupHeartbeatRequest`
  joins a new group, then a second heartbeat follows. A heartbeat response
  with no new assignment decodes with `assignment` equal to `null`, and its
  other fields (`errorCode`, `memberId`, `memberEpoch`) have the right
  values. Before the fix the decode fails or gives garbage. The response
  with an assignment still decodes with the partitions of the topic.
- If the Kafka 4.x broker of `compose.yaml` cannot show one of the two last
  scenarios, say so in the report. Do not change the broker for it without
  asking.
- The existing tests pass without changes, except expected generated code
  in `GeneratorTest` that has the old marker.

## Acceptance criteria

- [ ] `ApiKey::AddPartitionsToTxn` has the client range 0-3.
- [ ] `ApiKey::InitProducerId` has the client range 0-5.
- [ ] A bad entry of the client version list fails the generator.
- [ ] No generated class uses `writeBool` or `readBool` as the null marker
      of a struct.
- [ ] A null struct goes to a Kafka 4.x broker and comes back from it
      correctly.
- [ ] `src/Protocol/Generated/` is equal to the output of `make generate`.
- [ ] `make check` passes.

## Out of scope

- Default values of nullable fields that are absent in a version. See
  "Decisions".
- Nullable structs in tagged fields.
- Share groups, the new consumer group protocol and streams groups. The
  heartbeat requests are used here only as raw requests in a test.
- The transactional producer (task 003).
- New error codes of Kafka (task 043).

## Depends on

nothing
