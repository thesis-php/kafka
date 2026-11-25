---
name: implement
description: Implement a task with the implementer subagent, then review it in a loop
argument-hint: <task description>
disable-model-invocation: true
---
Task: $ARGUMENTS

If the task is empty or unclear, ask the user what to implement and stop.

You are the reviewer. You do not edit code in this command.

## 1. Before you start

Run `git status --short` and `git rev-parse HEAD`. Remember the result. You need
it later to tell the subagent's changes from changes that were already there.

## 2. Implement

Launch the `implementer` subagent in the background with the Agent tool. Its
prompt must contain:

- the full task text, word for word;
- "Read `AGENTS.md` first and follow it.";
- "Do not commit and do not push.";
- "Before you finish, run `make fix` and `make check`. Report the real result
  of each command."

## 3. Review

When the subagent finishes:

1. Read the full diff (`git status --short`, `git diff`, and new files). Do not
   rely on the subagent's report.
2. Run `make check` yourself.
3. Check the diff against the task and against `AGENTS.md`.

A finding is something that must be fixed:

- a bug or a missed edge case;
- the change does not do what the task asks, or does more than it asks;
- a rule from `AGENTS.md` is broken;
- a check is weakened to pass (deleted or skipped test, new ignore rule);
- `make check` fails.

Style preferences that `AGENTS.md` does not cover are not findings.

## 4. Fix loop

If there are findings, send them to the **same** subagent with SendMessage. Do
not start a new subagent: it would lose the context. Give each finding as
`file:line`, what is wrong, and what you expect. Then review again from step 3.

Stop when `make check` passes and there are no findings, or after 3 review
rounds.

## 5. Report

Tell the user:

- what was done;
- what changed because of the review;
- what is still open, with the failing output if a check does not pass.

Do not commit.
