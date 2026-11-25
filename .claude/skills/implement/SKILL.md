---
name: implement
description: Implement a task from the journal with the implementer subagent, then review it in a loop
argument-hint: <task number or title>
disable-model-invocation: true
---
Task: $ARGUMENTS

You are the reviewer. You do not edit code in this command. The rules of the
journal are in the "Journal" section of `AGENTS.md`. Read them first.

## 1. Find the task

1. Find the journal directory as `AGENTS.md` describes ("Which journal to
   use"). Read its `README.md`.
2. Find the task by its number or by its title. If the argument is empty,
   take the first task in the list that is not done.
3. Stop and tell the user if:
   - no task matches, or several tasks match the title;
   - the task is already done (`[x]`);
   - the task has no task file. Suggest `/plan-task <number>`;
   - a task from its "Depends on" section is not done.
4. Read the task file completely.

## 2. Before you start

Create the branch for the task. Its name is `<type>/<task>`:

- `<type>` is the "Type" line of the task file: `feat`, `fixes` or `chore`;
- `<task>` is the name of the task file without `.md`.

Example: `feat/001-transactional-producer`.

Run `git branch --show-current`, then:

- on the base branch of the journal: `git switch -c <type>/<task>`;
- already on the branch of this task: stay on it;
- the branch of this task exists, but you are not on it: `git switch` to it;
- on any other branch: stop and ask the user from where to branch.

Do not stash, reset or discard anything. Changes that are not committed stay
in the working tree and move to the new branch with you.

Then run `git status --short` and `git rev-parse HEAD`. Remember the result.
You need it later to tell the subagent's changes from changes that were
already there.

## 3. Implement

Launch the `implementer` subagent in the background with the Agent tool. Its
prompt must contain:

- the path of the task file, and "Read this file completely. It is the full
  description of the task. Do exactly what it says.";
- "Read `AGENTS.md` first and follow it.";
- "If the task file is wrong or something in it cannot be done, stop and
  report it. The internal design is yours, but do not change the
  'Decisions' section on your own.";
- "Do not edit the journal. Do not commit, do not push and do not switch
  branches.";
- "Before you finish, run `make fix` and `make check`. Report the real result
  of each command."

## 4. Review

When the subagent finishes:

1. Read the full diff (`git status --short`, `git diff`, and new files). Do not
   rely on the subagent's report.
2. Run `make check` yourself.
3. Check the diff against the task file, section by section, and against
   `AGENTS.md`. Go through every item of "Tests" and "Acceptance criteria".

A finding is something that must be fixed:

- a bug or a missed edge case;
- a step, a test or an acceptance criterion from the task file is missing;
- the change does something the task file does not ask for, or something
  from "Out of scope";
- the change differs from the "Decisions" section;
- a rule from `AGENTS.md` is broken;
- a check is weakened to pass (deleted or skipped test, new ignore rule);
- `make check` fails.

Style preferences that `AGENTS.md` does not cover are not findings.

## 5. Fix loop

If there are findings, send them to the **same** subagent with SendMessage. Do
not start a new subagent: it would lose the context. Give each finding as
`file:line`, what is wrong, and what you expect. Then review again from step 4.

Stop when `make check` passes and there are no findings, or after 3 review
rounds.

If the subagent reports that the task file is wrong, stop the loop and bring
the problem to the user. The task file is changed only with the user.

## 6. Report

Tell the user:

- which task was implemented (number, title) and on which branch;
- what was done;
- what changed because of the review;
- what is still open, with the failing output if a check does not pass.

Do not commit and do not mark the task as done. The task is marked when the
user asks to commit it, as `AGENTS.md` describes ("Finishing a task").
