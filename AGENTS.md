# AGENTS.md

Rules for AI agents that work in this repository. Read this file before you
change anything.

## What this project is

`thesis/kafka` is a Kafka client for PHP 8.4+. It is pure PHP and non-blocking.
It is built on `amphp` and `revolt/event-loop`. There is no `librdkafka` and no
PHP extension behind it: we speak the Kafka wire protocol ourselves.

## Repository layout

| Path | What is inside |
|------|----------------|
| `src/` | The library. Namespace `Thesis\Kafka\`. |
| `src/Protocol/Generated/` | Generated protocol classes. **Never edit by hand.** |
| `src/**/Internal/` | Private code. Not part of the public API. |
| `generator/` | Code generator for the protocol classes. |
| `schemas/messages/` | Kafka JSON schemas, copied from the Apache Kafka repository. |
| `tests/` | Tests. Same namespace as `src/`. Framework: Testo. |
| `examples/` | Small runnable scripts for users. |
| `docs/` | Plans and design notes. |
| `var/` | Caches and logs. Ignored by git. |

## How to run things

Everything runs in Docker. Do not run PHP tools on the host: the tests use
broker host names (`kafka-1:9092`, ...) that exist only inside the Docker
network, and the tools (PHPStan, Rector, PHP-CS-Fixer) live in the `php` image.

Use `make`:

| Command | What it does |
|---------|--------------|
| `make install` | Install Composer dependencies. |
| `make up-test-services` | Start all Kafka brokers needed for tests. |
| `make test` | Start the brokers and run all tests. |
| `make test ARGS='--filter=ProducerTest'` | Run one test class. |
| `make test ARGS='--filter=lingerDelaysFutureResolution'` | Run one test method. |
| `make test ARGS='--path=tests/Producer'` | Run one file or directory. |
| `make fix` | Fix code style (PHP-CS-Fixer, Rector, composer normalize). |
| `make phpstan` | Static analysis. |
| `make check` | Everything CI runs: style, composer checks, PHPStan, tests. |
| `make generate` | Rebuild `src/Protocol/Generated/` from the schemas. |
| `make sync-schemas` | Download schemas of the latest Kafka release. |
| `make run CMD='php examples/produce-record.php'` | Run any command in the `php` container. |
| `make down` | Stop all containers. |

Notes:

- The `php` image has no `bash`. `make terminal` does not work. Use
  `make run CMD='...'`.
- Every tool accepts extra arguments through `ARGS`, for example
  `make phpstan ARGS='src/Producer'`.
- A filter that matches nothing still ends with `PASSED` and `0 tests`. Check
  the test count in the summary.
- There is no `composer.lock`. CI tests with the lowest and the highest
  dependency versions, on PHP 8.4 and 8.5.

## Generated protocol code

- Files in `src/Protocol/Generated/` have the `@generated` tag. Do not edit
  them. Your changes will be lost on the next `make generate`.
- If a generated class is wrong or something is missing, fix the generator in
  `generator/`, then run `make generate`. Commit the generator change and the
  regenerated files together.
- The generator has its own test: `tests/Generator/GeneratorTest.php`. Add a
  case there when you change the generator.
- Do not edit files in `schemas/messages/` by hand. They come from
  `make sync-schemas`.
- Before you use a request or response class, open it and read the real field
  names and types. Do not guess them.

## How to write code

### General

- Every file starts with `declare(strict_types=1);`.
- Classes are `final`. Use `readonly` where the state does not change.
- Use enums, not string or int constants.
- Use constructor promotion and named arguments for calls with many parameters.
- Give precise PHPDoc types: `non-empty-string`, `list<T>`, `int<0, max>`,
  generics. PHPStan runs at `max` level with strict rules.
- Do not add `@phpstan-ignore` and do not add entries to `ignoreErrors` in
  `phpstan.dist.neon`. Fix the type instead.
- Match the code around you. Read two or three neighbour files before you write
  a new one.
- Keep changes small. Do not refactor code that is not part of your task.

### Public API and internal code

- Every class has `@api` or `@internal` in its PHPDoc.
- `@api` means users can depend on it. Be careful: every `@api` class is a
  promise. Do not add public API that the task does not need.
- Implementation goes to an `Internal` namespace and gets `@internal`.
- Public types are interfaces (`Producer`, `Consumer`). Their implementations
  are internal (`Producer\Internal\AsyncProducer`).
- Do not change an existing `@api` signature without asking the user.

### Async code

- Never block the event loop. No `sleep()`, `usleep()`, blocking sockets or
  blocking file reads in `src/`. Use `Amp\delay()`, `Amp\async()`, `Future`,
  `DeferredFuture`, and `amphp/sync` primitives.
- Long operations accept an `Amp\Cancellation`.
- `close()` methods are idempotent. They stop background tasks and release
  connections.
- Think about what happens when two coroutines call the same method at once.

### Errors

- All library exceptions extend `Thesis\Kafka\Exception`. They live in
  `src/Exception/`, one class per failure, with public readonly context
  properties.
- Do not throw generic `\RuntimeException` or `\LogicException` to users.
- Map Kafka error codes through `Protocol\ErrorCode` and the enums in
  `Protocol\Error\`. Do not compare with raw numbers other than `0` (no
  error).

### Comments

- Write short comments in plain English. One idea per sentence.
- No long sentences with many clauses, no idioms. Not every reader is a native
  English speaker.
- A comment explains *why*. Do not describe *what* the code does.
- Kafka terms (epoch, offset, fence, coordinator) are fine.

### Logging

- Log through the PSR-3 logger that the class already has.
- Put the log call inline, at the place where the event happens.
- Do not write a comment that explains a log call.
- Do not create helper methods only for logging.
- Use the same context keys as the existing logs (`topics`, `partition`, ...).

### Dependencies

- Do not add a Composer dependency without asking the user.
- If you add one, `make deps-analyze` and `make composer-normalize-check` must
  pass.

## How to write tests

- Tests are **integration tests**. They talk to real brokers from
  `compose.yaml`. No mocks of the broker, the connection or the cluster.
- Test through the public API: `Client`, `Producer`, `Consumer`. Use raw
  protocol requests (`$client->request(...)`) only to prepare or to check
  broker state.
- Do not make a method `public` or `static` only to test it. Do not add test
  hooks to `src/`.
- Do not write unit tests for internal classes. If internal code cannot be
  reached through the public API, it is probably dead code. Delete it.
- A test class is `final readonly`, extends `Thesis\Kafka\TestCase`, and marks
  methods with `#[Test]`.
- Add `#[Covers(SomeClass::class)]` for the classes the test covers.
- Tests that depend on broker timing get
  `#[Retry(maxAttempts: 5, markFlaky: true)]`. Do not use retry to hide a real
  bug.
- Each test creates its own topic with `createTopic()` and waits for it with
  `waitUntilTopicVisible()`. Use a unique group id per test. Tests must not
  depend on each other or on the run order.
- Clean up at the end: delete the topic, close the producer, the consumer and
  the client.
- Assert with `Testo\Assert`. Assert real values (topic, partition, offset,
  record value), not only "no exception".
- Test failure paths too: broker errors, rebalances, fencing, closed
  connections, cancellation.
- If a feature needs a new broker setup (new Kafka version, new listener), add
  a service to `compose.yaml` and to `TEST_SERVICES` in the `Makefile`.

## Quality checklist

Run this before you say the work is done:

1. `make fix` — formats the code. Read the diff it makes.
2. `make phpstan` — must report no errors.
3. `make test` — must pass. Run it twice if you touched timing-sensitive code.
4. `make check` — the full CI run. Must be green.

Rules:

- A check that fails is your problem to fix, even if your change looks
  unrelated. If it really was broken before you started, say so in your report
  and show the output.
- Do not weaken a check to make it pass: no deleted tests, no skipped tests, no
  lowered levels, no new ignore rules.
- Report honestly. If you did not run something, write that you did not run it.

## Mutation testing (Infection)

The goal is 100% MSI on `src/`. The config is `infection.json5.dist`. Run it
with `make infect`.

- Kill a mutant with a better integration test. Assert the behaviour the
  mutant breaks.
- If a mutant cannot be killed through the public API, the code is probably not
  needed. Simplify or remove the code.
- Do not disable mutators and do not lower `minCoveredMsi` to get a green run.
  Ignore a mutant only when it is truly equivalent, and write the reason next
  to the ignore rule.
- Generated code (`src/Protocol/Generated/`) is a special case. Ask the user
  before you decide how to treat it.

## Documentation

- A new user-facing feature needs a section in `README.md` and a runnable
  script in `examples/`.
- `README.md` uses the same plain English as the code comments.
- Only `src/`, `examples/` and a few root files ship in the Composer package
  (see `.gitattributes`).

## Git

- Do not commit or push unless the user asks.
- Commit messages follow the existing history: `chore: short description`,
  `fix: ...`, `feat: ...`.
- One logical change per commit.

## When to stop and ask

Ask the user before you:

- change or remove an existing `@api` signature;
- add a Composer dependency;
- change `compose.yaml` broker versions that existing tests use;
- cannot make a check pass without weakening it.
