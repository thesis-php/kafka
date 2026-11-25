---
name: plan-task
description: Write a task for the journal (docs/journal) together with the developer. Use it when the developer wants to plan a fix, a feature or an improvement before it is implemented.
argument-hint: <idea, or the number of a task that has no file yet>
---
Input: $ARGUMENTS

You write one task for the journal. You do not implement it. The rules of the
journal are in the "Journal" section of `AGENTS.md`. Read them first.

The result is a task file that another agent can implement without asking
questions.

## 1. Find the journal

Find the journal directory as `AGENTS.md` describes ("Which journal to use").
Read its `README.md`.

- If the input is a number or a title of a task from the list, you write the
  file for that task.
- If the input is a new idea, you add a new task. Its number is the highest
  number in the list plus one, with three digits.
- If the input is empty, ask the developer what to plan.

## 2. Study the code

Before you ask anything, read the code the task touches. Read the generated
protocol classes and the schema files in `schemas/messages/` when the task
uses the protocol. Read the task files it depends on.

Every path, class, method and field you write in the task must exist. Check
each one. Do not write names from memory.

## 3. Talk with the developer

This step is required. Do not skip it, even if the task looks clear.

1. Say in a few sentences how you understand the task and what you found in
   the code.
2. Ask about every decision that the code cannot answer: public API names and
   signatures, behaviour in edge cases, error handling, what is out of scope.
   Use AskUserQuestion. Give your recommended option first.
3. Do not ask what you can find in the code yourself.
4. Repeat until no open decision is left. The task file must contain
   decisions, not questions.

If the task is too big for one commit, propose how to split it into several
tasks and agree on the split before you write anything.

## 4. Write the task

1. Copy `docs/journal/TEMPLATE.md` to
   `docs/journal/<branch>/NNN-short-title.md`. Fill every section. Write
   "nothing" where a section is empty; do not delete sections.
   Set the "Type" line to one value: `feat`, `fixes` or `chore`. Remove the
   comment under it. `/implement` uses the type as the branch prefix.
2. Write in plain English, as `AGENTS.md` asks for comments.
   Describe the idea, not the implementation. Do not list the classes to
   create or change, and do not write step-by-step code instructions. Fix
   only what was agreed with the developer: public API, behaviour, error
   handling. The internal design is left to the implementer and the review.
3. Add the task to the list in the journal `README.md`, or turn the existing
   line into a link:
   `- [ ] [NNN Short title](NNN-short-title.md)`.
   Put it where it belongs in the order of work, after the tasks it depends
   on. Do not renumber existing tasks.
4. Remove the "No tasks yet" line if it is still there.

## 5. Finish

Show the developer the path of the task file and a short summary: goal, main
decisions, tests. Ask them to read the file.

Do not start the implementation. Do not commit unless the developer asks.
