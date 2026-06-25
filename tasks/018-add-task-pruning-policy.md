# Add a lifecycle policy for implementation tasks

**Priority:** P3 repository hygiene  
**Primary area:** `tasks/README.md`, task workflow  
**Dependencies:** none

## Problem

The `tasks/` directory is useful as an implementation backlog, but without a
lifecycle policy it can become stale. Completed tasks may remain in the
recommended order, partially implemented tasks may not reflect current code, and
new reviewers may not know whether a task is still valid.

## Objective

Define a lightweight policy for keeping implementation tasks current.

## Required behavior

Update the task index or add a task policy document that explains:

- how to mark a task as open, in progress, superseded, or complete;
- when to delete a task versus keep it as historical context;
- how an implementation PR should update or remove its corresponding task;
- how to split a large task if implementation uncovers new work;
- how to cite benchmark evidence without committing raw JSON;
- how often to re-review `main` and refresh the task list.

## Acceptance criteria

- Contributors know how to close out task files.
- Completed tasks do not remain indistinguishable from open work.
- The task directory remains useful as an active backlog rather than stale
  documentation.

## Non-goals

- Building a full issue tracker replacement.
- Requiring every small code change to have a task file.
