# 038 Integration test kit

Type: chore

## Goal

Integration tests get a small test kit: a facade that connects to a test
broker, creates topics, waits for them, gives clients and cleans up after the
test. A test contains only its scenario and its assertions. If the kit proves
to be good, a later task can give it to the users of the library as public
API.

## Context

- `tests/TestCase.php` has the helpers of today: `createClient()`,
  `topicName()`, `createTopic()`, `deleteTopic()`, `waitUntilTopicVisible()`
  and `metadataRequest()`. Every test class extends `TestCase`.
- About 20 tests repeat the same frame: create a client, create a topic, wait
  for the topic, run the scenario, close the consumer and the producer,
  delete the topic, close the client. The number of partitions is written
  twice: for the creation and for the wait.
- The cleanup is the last statement of the test. When the test fails before
  it, the topic and the open clients stay. Only two places in
  `tests/DirectConsumerTest.php` use `finally`. Task 037 describes what the
  leaked topics do to the brokers.
- `TestCase::waitUntilTopicVisible()` asks one broker for the metadata. The
  main test cluster has three brokers (`kafka-1`, `kafka-2`, `kafka-3`), and
  each of them learns about a new topic at its own time. The test can go on
  while the leader of a partition does not know the topic yet. This is a
  probable reason of the `UnknownTopic` and `UnknownTopicOrPartition`
  failures in the flaky tests. It is a guess from the code; it was not
  measured.
- `TestCase::waitUntilTopicVisible()` waits with the blocking `usleep()`.
- Helpers are copied between test classes:
  - reading N records: `DirectConsumerTest::consumeUntilCount()` tries 10
    times, `GroupConsumerTest::consumeUntil()` loops without a limit. The
    second one never ends when the records do not come;
  - `GroupConsumerTest::createGroupConsumer()` and
    `GroupConsumerTest::generateGroupId()`;
  - `AdminClientTest::waitUntil()` and
    `AdminClientTest::waitUntilTopicConfigsVisible()`;
  - `ClusterTest::waitUntilClusterMetadataContainsTopics()` and
    `ClusterTest::waitUntilClusterMetadataDropsTopics()`;
  - a `create...Client()` method with the broker address in each SASL test.
- The test brokers are started by Docker from `compose.yaml`: the main
  cluster, `kafka-plain-standalone`, `kafka-plain-reauth-standalone`,
  `kafka-scram-256-standalone`, `kafka-scram-512-standalone` and
  `kafka-apiversions-legacy`. The addresses of the main cluster are in
  `TestCase::SEEDS`. A test of another broker passes its address to
  `createClient()`.
- Many test classes have `#[Retry(maxAttempts: 5, markFlaky: true)]`. Every
  attempt is a new call of the test method.
- PHP allows to switch fibers in a destructor since 8.4. The project needs
  PHP 8.4 or newer.
- Only `src/` and `examples/` ship in the Composer package (see
  `.gitattributes`). Code in `tests/` is not public.

## What to do

Write a test kit in `tests/`. The test makes one environment object with a
static factory. It asks this object for everything it needs: a topic, a
producer, a consumer, a client for raw requests. The environment remembers
what it gave. When the test ends, the environment closes the clients and
deletes the topics in its destructor, also when the test failed.

The kit does not start brokers. Docker does it. The kit only connects to a
broker that already runs.

The agreed look of a test:

```php
#[Test]
public function readAfterCommit(): void
{
    $kafka = Kafka::connect();

    $topic = $kafka->topic(partitions: 2);
    $producer = $kafka->producer();
    $consumer = $kafka->groupConsumer($topic);

    $producer->produceSync([...]);
    $records = $kafka->consume($consumer, count: 4);

    Assert::count($records, 4);
}
```

The names in this example show the idea. The implementer may choose better
names and signatures, but the test must stay this short: no base class, no
`close()`, no `deleteTopic()`, no separate wait for the topic.

Move all tests that talk to a broker to the kit. Then delete `TestCase` and
the copied helpers in the test classes.

Update `AGENTS.md` ("How to write tests") to describe the new way.

## Decisions

- The kit lives in `tests/` and is `@internal`. Nothing is added to `src/`
  and nothing becomes `@api` in this task.
- The kit does not depend on Testo. It is plain objects that work with
  `Thesis\Kafka\Client`. This keeps the way open to move it to `src/` later.
- No inheritance. A test gets the environment from a static factory.
- The factory has default broker addresses: the main test cluster. A test can
  pass other addresses and a SASL config, for example for a SASL broker or
  for the old broker. There is no enum of brokers.
- `TestCase` is deleted. Test classes are `final readonly` and have no
  parent, also the classes that do not talk to a broker. `SEEDS`,
  `CLUSTER_ID` and `metadataRequest()` move to the kit or to the tests that
  need them.
- The kit contains:
  - topics: one call creates a topic with a unique name and waits until it
    is ready;
  - clients: a producer, a direct consumer, a group consumer and a client for
    raw protocol requests;
  - a group consumer with a unique group id and short session timeouts, as
    `GroupConsumerTest::createGroupConsumer()` has today;
  - reading N records from a consumer with a time limit;
  - one general wait for a condition with a time limit.
- A topic is ready when every broker of the cluster sees it with all its
  partitions and a leader for each partition. One broker is not enough.
- The tests stay on the three-broker cluster. They do not move to a single
  broker.
- A topic that a test creates with a raw request (`AdminClientTest` tests the
  create and delete requests themselves) can be registered in the environment
  for cleanup.
- Every wait in the kit has a time limit. The default is 10 seconds, and a
  call can change it. When the time is over, the test fails with a message
  that says what was expected and what was seen. No wait loops forever.
- Waits do not block the event loop: `Amp\delay()`, not `usleep()`.
- Cleanup runs in the destructor of the environment. There is no hook and no
  Testo plugin. The kit keeps no strong reference to the environment, so the
  destructor runs when the test method ends.
- First of all, check that cleanup with network requests works in a
  destructor: after a passed test, after a failed test and after a failed
  `#[Retry]` attempt, on PHP 8.4 and 8.5. If it does not work, stop and
  report it. Do not build another way on your own.
- Cleanup tries to release everything, also when one step fails.
- When cleanup fails, the destructor throws. A passed test then fails with
  the cleanup error. For a failed test PHP keeps the error of the test as
  `previous` of the cleanup error. No error is lost.
- All tests that talk to a broker move to the kit in this task. There is one
  way to write a test.
- The scenarios and the assertions of the existing tests do not change. Only
  the setup, the waits and the cleanup change.

## Tests

No new test classes: the kit is test code, and the existing tests use every
part of it. Check the result with these runs:

- Break one assertion in a test by hand, run this test, then list the topics
  on the broker: the topic of the test is gone. Revert the change.
- Do the same for a test with `#[Retry]`: no attempt leaves a topic.
- Make a consumer test expect one record more than it produces: the test
  fails after the time limit with a clear message. It does not hang. Revert
  the change.
- After `make test` on clean brokers no test topic stays on any broker.
- `make test` passes five times in a row on brokers that were just created
  (`make down` before each run). The report gives the number of flaky tests
  in each run, before and after this task.

## Acceptance criteria

- [ ] No test that talks to a broker creates, waits for or deletes a topic by
      hand, and none closes its clients by hand.
- [ ] `tests/TestCase.php` does not exist, no test class has a parent, and
      the test classes have no copies of the kit helpers.
- [ ] A failed test and a failed retry attempt leave no topic and no open
      client.
- [ ] No wait in the tests is without a time limit, and none uses `usleep()`.
- [ ] A test of the main cluster does not write broker addresses. Only a
      test of another broker passes them.
- [ ] The kit has no dependency on Testo.
- [ ] `AGENTS.md` describes the new way to write a test.
- [ ] `make check` passes.

## Out of scope

- Public API for users: moving the kit to `src/`, `README.md` and
  `examples/`. It is a new task after we have used the kit for some time.
- Changes to test scenarios or assertions, and new test scenarios.
- Changes to `compose.yaml`: no new broker, no single-broker setup.
- Clean brokers for `make infect` and the new measurement. This is task 037.
- The Kafka 4.x broker. Task 002 adds it; its tests pass its address to the
  factory.
- Consumer groups that tests leave on the brokers.
- Fixing flaky tests in `src/`. A test that hung in a wait without a limit
  now fails with a message; a bug of the client behind it is not fixed here.
- Mutants that Infection kills by a timeout. The process is killed, so no
  destructor runs. Task 037 covers them.

## Depends on

001
