# 001 Mutation testing scope

Type: chore

## Goal

Infection must mutate all hand-written code that the tests run, not only a few
classes. After this task we know the real MSI of the project and the number of
escaped mutants in each area. We need these numbers to plan the next tasks.

## Context

- Infection runs with `make infect`. The config is `infection.json5.dist`. It
  uses the Testo adapter from `tools/infection/composer.json`.
- Testo collects coverage only for the classes named in `#[Covers]`. Every
  test class has this attribute today. Infection creates mutants only for
  covered lines, so only 12 files are mutated: 424 mutants, MSI 66-68%.
- Some attributes point to code that cannot be covered. For example,
  `tests/DirectConsumerTest.php` covers the `Consumer` interface.
- A trial run without `#[Covers]` created 3506 mutants in 167 files. 1557 of
  them are in `src/Protocol/Generated/`. About 1950 are in hand-written code.
  That run did not execute the tests, so the number of escaped mutants is not
  known yet.
- `src/` has 53 logger calls. Many of them pass a context array that takes
  several lines.
- `AGENTS.md` tells agents to add `#[Covers]` to tests ("How to write tests")
  and says that generated code is an open question ("Mutation testing").
- The `infect` job in `.github/workflows/check.yaml` runs
  `make infect ARGS='--threads=max'`.

## What to do

Remove the `#[Covers]` attributes from the tests, so that coverage includes
every line a test runs.

Change the Infection config so that it does not mutate generated protocol code
and does not report mutants in logging code.

Run Infection on the whole project and set `minCoveredMsi` to the real level.
The threshold must pass in a normal run. It is raised in later tasks.

Update `AGENTS.md` to match the new rules.

Do not kill mutants in this task. This task only sets the scope and measures.

## Decisions

- `#[Covers]` is removed from all test classes. New tests do not use it.
- `src/Protocol/Generated/` is excluded from Infection. The goal of 100% MSI
  is about hand-written code.
- Mutants in logging code are ignored in the config, with a comment that gives
  the reason. Tests do not check log messages or log context. This covers the
  logger calls and the context arrays passed to them, also when the array is
  on other lines than the call, and the context of `ContextLogger::with()`.
  Infection has a `global-ignoreSourceCodeByRegex` setting, but it matches one
  source line. If the config cannot ignore a whole multi-line call, stop and
  report it. Do not change the code style of logger calls to make it work.
- No other mutator is disabled and no other code is ignored.
- `minCoveredMsi` is set a few points below the lowest MSI of three full
  runs. Mutants killed by a timeout change from run to run, so the threshold
  needs this margin.
- In `AGENTS.md`: the rule "Add `#[Covers]`" becomes "Do not add `#[Covers]`",
  with the reason. The "Mutation testing" section says that generated code is
  excluded and logging code is ignored.

## Tests

No new tests. Check the result with these runs:

- `make infect` passes three times in a row with the new threshold.
- `make check` passes.
- The Infection log has no mutant from `src/Protocol/Generated/` and no mutant
  in a logger call or its context.

## Acceptance criteria

- [ ] No test class has `#[Covers]`.
- [ ] Infection does not create mutants for `src/Protocol/Generated/`.
- [ ] Infection does not report mutants in logging code. The config explains
      why.
- [ ] `minCoveredMsi` is set from three measured runs, and `make infect`
      passes.
- [ ] `AGENTS.md` describes the new rules.
- [ ] The final report has a table for each area (hand-written protocol code,
      connection and cluster, producer, consumer, the rest): mutants, killed,
      escaped, timed out. It also gives the duration of `make infect`.
- [ ] `make check` passes.

## Out of scope

- Killing escaped mutants. It is planned after this task, from the measured
  numbers.
- Mutation testing of the generator (`generator/`).
- Changes to the tests other than removing `#[Covers]`.
- Making the test suite faster or less flaky.

## Depends on

nothing
