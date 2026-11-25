# NNN Short title

Type: feat | fixes | chore

<!--
Keep one value. It is the prefix of the task branch:
feat  — a new feature;
fixes — a bug fix;
chore — technical debt, a small change, maintenance.
-->

## Goal

What must exist when this task is done, and why we need it. Two or three
sentences.

## Context

What the reader must know before starting:

- how the related code works today, and where to start reading;
- links to the Kafka documentation or KIPs.

## What to do

The idea of the solution, in a few paragraphs: what changes in the behaviour
and how it should work. Do not list every class to create or change. The
implementer chooses the internal design.

## Decisions

Things already agreed with the developer. The implementer must not decide them
again. Keep only the points that matter for this task:

- public API: names and signatures of new or changed `@api` items;
- behaviour in edge cases;
- error handling: what is retried, what is thrown to the user.

## Tests

The integration scenarios to add. One line per scenario: setup, action,
expected result.

- ...

## Acceptance criteria

- [ ] ...
- [ ] `make check` passes.

## Out of scope

Things that look related but are not part of this task.

## Depends on

Task numbers that must be done first, or "nothing".
