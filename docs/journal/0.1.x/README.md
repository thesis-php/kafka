# Journal 0.1.x

Tasks for the `0.1.x` branch. The rules are in the "Journal" section of
[AGENTS.md](../../../AGENTS.md).

## Goals

In this branch we do three things:

- make the existing code stable: fix the bugs found by the audit and the
  flaky tests;
- make it fast: remove the known performance problems of the read and write
  paths;
- add transactions: the transactional producer, `ReadCommitted` consumers and
  the transact session.

## Tasks

The findings behind tasks 009-031 are in [audit-findings.md](audit-findings.md).

<!--
One line per task, in the order of work:

- [ ] [001 Short title](001-short-title.md)
- [x] [002 Short title](002-short-title.md) — [a1b2c3d](https://github.com/thesis-php/kafka/commit/a1b2c3d)
- [ ] 003 Short title          (no task file yet)
-->

- [x] [001 Mutation testing scope](001-mutation-testing-scope.md) — [93a8aac](https://github.com/thesis-php/kafka/commit/93a8aac)
- [x] [038 Integration test kit](038-integration-test-kit.md) — [e4ac3fc](https://github.com/thesis-php/kafka/commit/e4ac3fc)
- [ ] [037 Test topic leak](037-test-topic-leak.md)
- [x] [002 Kafka 4 test broker](002-kafka-4-test-broker.md) — [13593d3](https://github.com/thesis-php/kafka/commit/13593d3)
- [x] [008 Produce by topic id](008-produce-by-topic-id.md) — [4aa3a83](https://github.com/thesis-php/kafka/commit/4aa3a83)
- [x] [013 Topic lookup in metadata](013-topic-lookup.md) — [f171341](https://github.com/thesis-php/kafka/commit/f171341)
- [x] [014 Cancellation and the metadata timer in Cluster](014-cluster-cancellation.md) — [a384054](https://github.com/thesis-php/kafka/commit/a384054)
- [x] [015 Connection failures and leaks](015-connection-failures.md) — [6cb4f0b](https://github.com/thesis-php/kafka/commit/6cb4f0b)
- [x] [021 Group close and rebalance](021-group-close-and-rebalance.md) — [556e220](https://github.com/thesis-php/kafka/commit/556e220)
- [x] [039 Shared work in consumers and cancellation](039-shared-work-in-consumers.md) — [538347a](https://github.com/thesis-php/kafka/commit/538347a)
- [x] [040 Metadata timer: interval and log](040-metadata-timer-details.md) — [6171caf](https://github.com/thesis-php/kafka/commit/6171caf)
- [x] [010 Varint and int64 without copies](010-varint-decoding.md) — [e3a9669](https://github.com/thesis-php/kafka/commit/e3a9669)
- [ ] [011 Retriable errors and leader change](011-retriable-errors.md)
- [ ] 012 Generator: client versions and null structs
- [ ] 016 Producer shutdown
- [ ] 017 Producer concurrency
- [ ] 018 Idempotent producer after a failed batch
- [ ] 019 Fetch errors
- [ ] 020 Consumer lifecycle
- [ ] 022 Group commits
- [ ] 009 Topic ids in offset commit
- [ ] 023 Group membership
- [ ] 024 Compression codecs
- [ ] 025 Send the client id
- [ ] 028 Compare the port of an endpoint
- [ ] 029 Timestamp of LogAppendTime records
- [ ] 030 Errors lost by Errors::for()
- [ ] 031 Library exceptions in place of generic ones
- [ ] 041 Requests that go to the controller
- [ ] 042 Connection and metadata leftovers
- [ ] 043 Error codes after 133
- [ ] 026 Write and read performance
- [ ] 027 One Produce request per broker
- [ ] [003 Transactional producer](003-transactional-producer.md)
- [ ] [004 Transaction errors and fencing](004-transaction-errors.md)
- [ ] [005 Read committed consumer](005-read-committed-consumer.md)
- [ ] [006 Consumer offsets in a transaction](006-offsets-in-transaction.md)
- [ ] [007 Transact session](007-transact-session.md)
- [ ] 032 TLS tests
- [ ] 033 Broker throttling
- [ ] 034 Consumer position and partition selection
- [ ] 035 New partitions of a topic
- [ ] 036 Release preparation

## Deferred to 0.2.x

Topics that are not part of 0.1.x. They have no numbers and no task files.
`/plan-branch 0.2.x` moves them to the new journal.

- Share groups (KIP-932). Needs a broker that supports them and the
  generator fix from task 012.
- The new consumer group protocol (KIP-848).
- Cooperative rebalancing (KIP-429). `Consumer\RebalanceProtocol::Cooperative`
  exists, but the join code does not support it.
- Metrics: local metrics for the user, and client telemetry for the broker
  (KIP-714).
- SASL OAUTHBEARER. Kerberos (GSSAPI) is not planned.
- A typed admin client. Today there is only `Client::request()`.
- Fetch sessions (KIP-227).
- Fetch from the closest replica (KIP-392). The rack id is always empty
  today.
- Protection from a stuck handler: a consumer whose handler never returns
  keeps its partitions forever, because the heartbeat runs in the
  background. There is no `max.poll.interval` equivalent.
- Rebootstrap when all known broker addresses changed (KIP-899).
