# 037 Test topic leak

Type: chore

## Goal

`make infect` must always measure on clean brokers. Today the leftover topics
slow the brokers down and make the numbers of task 001 wrong. After this task
we repeat the measurement and have numbers we can trust.

## Context

- Every test creates its own topic with `createTopic()` from
  `tests/TestCase.php` and deletes it with `deleteTopic()`.
- In most tests `deleteTopic()` is the last statement of the method. See
  `tests/GroupConsumerTest.php`, `tests/ProducerTest.php`,
  `tests/AdminClientTest.php` and `tests/Internal/ClusterTest.php`. When an
  assertion fails before it, the topic stays. Only two places in
  `tests/DirectConsumerTest.php` use `finally`.
- Many test classes have `#[Retry(maxAttempts: 5, markFlaky: true)]`. Every
  failed attempt leaves one more topic.
- Infection makes this much worse. A killed mutant is a failed test. A mutant
  that is killed by a timeout is a killed process: no code of the test runs
  after that, so cleanup in the test cannot help there.
- Task 038 moves all tests to a test kit that deletes the topics also when a
  test fails. This task starts after it.
- `make infect` depends on `up-test-services` in the `Makefile`. It starts the
  brokers but does not clean them. The broker data is inside the containers
  (`/tmp/kafka-logs`) and lives until `make down`.
- What we saw in task 001, after five `make infect` runs in a row:
  - about 29 000 partition directories on each of `kafka-1`, `kafka-2`,
    `kafka-3`;
  - `kafka-1` died with `OutOfMemoryError: Java heap space` and then failed
    its healthcheck on restart;
  - `make check` failed: first on `up-test-services`, then with
    `UnknownTopic` in `GroupConsumerTest::oneConsumerReadsEveryPartition`;
  - the test suite took 250 s, and 18 s after the brokers were created again.
- The numbers of task 001 were measured on these dirty brokers: about 1830
  mutants, 228-361 timed out and 268-495 skipped per run, MSI 58-64%.
  Infection counts a timed out mutant as killed. Without the timeouts the MSI
  is about 33-35%.
- `infection.json5.dist` has `minCoveredMsi` 55 from that measurement.
- The `infect` job in `.github/workflows/check.yaml` runs
  `make infect ARGS='--threads=max'` on fresh brokers.

## What to do

Make `make infect` start from clean brokers every time: it creates the test
brokers again before Infection starts. This covers the mutants that are
killed by a timeout. Failed tests are covered by the test kit of task 038.

Then repeat the measurement of task 001: run `make infect` three times in a
row, set `minCoveredMsi` from the result and build the table for each area.

Update `AGENTS.md` to match the new rules.

## Decisions

- Cleanup after a failed test is not a part of this task. Task 038 does it.
  This task does not change the tests.
- `make infect` always creates the test brokers again. It does not ask and
  has no switch. All data in the local test brokers is lost on every run.
- `make test` and `make check` do not touch the brokers. They work as today.
- The measurement follows the rules of task 001: three full runs,
  `minCoveredMsi` a few points below the lowest MSI. The comment next to
  `minCoveredMsi` is updated.
- The new threshold is the measured level, also when it is lower than 55.
  Fewer timeouts can mean a lower MSI, because Infection counts a timeout as
  a kill. This is a correction of a wrong measurement, not a weakened check.
- The Infection timeout and the number of threads are not changed. The report
  shows how many mutants are still timed out or skipped on clean brokers.
- No mutants are killed in this task.
- In `AGENTS.md`: the "Mutation testing" section says that `make infect`
  creates the test brokers again and deletes their data.

## Tests

No new tests. Check the result with these runs:

- `make infect` passes three times in a row with the new threshold.
- After the three runs all test brokers are healthy, and no broker log has
  `OutOfMemoryError`.
- `make check` passes right after the three runs, without `make down`.

## Acceptance criteria

- [ ] `make infect` starts from clean brokers on every run.
- [ ] `minCoveredMsi` is set from three measured runs on clean brokers, and
      `make infect` passes.
- [ ] `AGENTS.md` describes the new rules.
- [ ] The final report has the three MSI values and a table for each area
      (hand-written protocol code, connection and cluster, producer, consumer,
      the rest): mutants, killed, escaped, timed out, skipped. It compares
      them with the numbers of task 001 and gives the duration of
      `make infect`.
- [ ] `make check` passes.

## Out of scope

- Killing escaped mutants.
- Changes to the Infection timeout or to the number of threads. If many
  mutants are still timed out or skipped, it is a new task, planned from the
  numbers of this one.
- Consumer groups that tests leave on the brokers.
- The heap size and other broker settings in `compose.yaml`.
- The flaky tests found in task 001
  (`ProducerTest::fullBatchBypassesLingerAndResolvesImmediately`,
  `PlainSaslReauthenticationTest::closesConnectionIfExpiredSessionBypassesRefresh`).
- Changes to the tests.

## Depends on

001, 038
