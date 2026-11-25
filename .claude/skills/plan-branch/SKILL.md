---
name: plan-branch
description: Start the journal for a new base branch (for example 0.2.x) together with the developer. Use it when work on a new version begins.
argument-hint: <branch name, for example 0.2.x>
---
Branch: $ARGUMENTS

You create the journal for a new base branch: the directory
`docs/journal/<branch>/` with a `README.md` that holds the goals and the task
list. You do not write task files here and you do not implement anything. The
rules of the journal are in the "Journal" section of `AGENTS.md`. Read them
first.

## 1. Check the input

- The branch name must look like `0.2.x` (`<major>.<minor>.x`). If it is empty
  or looks different, ask the developer.
- If `docs/journal/<branch>/` already exists, stop and tell the developer. Do
  not overwrite it.
- Run `git branch --show-current` and `git branch --list '<branch>'`. The
  journal must be created on the new base branch. If you are not on it, ask
  the developer what to do: switch to it, create it from the current branch,
  or stay. Do not create or switch branches without a clear answer.

## 2. Read the previous journal

Find the journal of the previous version (the highest version directory in
`docs/journal/`). Read its `README.md` and collect the tasks that are not done
(`- [ ]`).

## 3. Talk with the developer

This step is required. Ask, and wait for answers:

1. What must this version deliver? Ask for the main goals.
2. Are breaking changes of the public API allowed in this version?
3. For each task that is not done in the previous journal: move it to the new
   journal, leave it for the old branch, or drop it? Show the list and use
   AskUserQuestion.
4. Which other tasks does the developer already know about? Collect them as
   short titles.
5. In which order should the tasks go?

Propose tasks yourself when you see something the goals need, but add them
only after the developer agrees.

## 4. Write the journal

1. Create `docs/journal/<branch>/README.md`. Use
   `docs/journal/0.1.x/README.md` as the example of the structure: title,
   link to `AGENTS.md`, "Goals", "Tasks".
2. Write the goals from the developer's answers. Note there if breaking
   changes are allowed.
3. Write the task list. Numbers start at `001` again. Each task is one line
   without a link: `- [ ] NNN Short title`. Task files are written later, one
   by one, with `/plan-task`.
4. For every task you moved, change its line in the previous journal to
   `- [ ] NNN Short title — moved to <branch> as MMM`. Remove its task file
   from the previous journal only after you copied it to the new one with the
   new number.
5. For every task the developer dropped, change its line in the previous
   journal to `- [ ] NNN Short title — dropped`.

## 5. Finish

Show the developer the new `README.md` and the changes in the previous
journal. Say which task to plan first with `/plan-task`.

Do not commit unless the developer asks.
