# 002 Kafka 4 test broker

Type: chore

## Goal

The test setup has a Kafka 4.x broker next to the Kafka 3.9 cluster. Tests can
choose which broker they talk to. We need this for transactions, because
Kafka 4.x uses a second version of the transaction protocol (KIP-890).

## Context

- `compose.yaml` has a three-node Kafka 3.9.0 cluster (`kafka-1`, `kafka-2`,
  `kafka-3`) and several single-node brokers for SASL and for an old broker
  (`kafka-apiversions-legacy`). The single-node brokers show how to add one
  more.
- The `Makefile` lists the services that tests need in `TEST_SERVICES`.
- `tests/TestCase.php` has the seeds of the main cluster in `SEEDS`, and
  `createClient()` accepts other seeds. `tests/LegacyApiVersionsBrokerTest.php`
  is an example of a test that uses another broker.
- The protocol classes in `src/Protocol/Generated/` come from the schemas in
  `schemas/messages/`. The broker version must not be newer than the Kafka
  release these schemas come from.
- KIP-890: https://cwiki.apache.org/confluence/display/KAFKA/KIP-890

## What to do

Add one single-node Kafka 4.x broker to the test setup. It must start with
`make up-test-services` and in CI. The new transaction protocol
(`transaction.version` 2) must be on for this broker; check it on the running
broker, do not assume the default.

Give tests a simple way to run the same scenario against the Kafka 3.9 cluster
and against the Kafka 4.x broker. The transaction tasks (003-007) use it.

Add one small test that connects to the new broker and proves that it works:
create a topic, produce one record, consume it.

## Decisions

- One single-node broker is enough. No second three-node cluster.
- Only transaction tests run against both brokers. The rest of the test suite
  stays on Kafka 3.9.
- The existing Kafka 3.9 services are not changed.
- Use the newest `apache/kafka` 4.x image that the schemas in
  `schemas/messages/` support. If the schemas are older than the image needs,
  stop and report it. Do not update the schemas in this task.
- The broker must support Produce version 13. Task 008 needs it. Check it
  with the versions the broker reports, and put the result in the task
  report.

## Tests

- A client connects to the Kafka 4.x broker, creates a topic, produces one
  record and consumes the same record.

## Acceptance criteria

- [ ] `make up-test-services` starts the Kafka 4.x broker and waits until it
      is healthy.
- [ ] Tests can run one scenario against both brokers without copying the
      test body.
- [ ] The new test passes.
- [ ] `transaction.version` 2 is on for the new broker, and the task report
      shows how it was checked.
- [ ] `make check` passes.

## Out of scope

- Running the whole test suite against Kafka 4.x.
- Share groups.
- Updating the schemas or the generated protocol classes.

## Depends on

nothing
