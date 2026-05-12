# Contributing

This file is intentionally narrow. It exists to codify one rule that has bitten
us in practice. As more rules accumulate they can be added below.

## Design artifacts belong in git

Design artifacts — HTML mockups, screenshots, exported PDFs, Figma exports —
are first-class engineering references. Track them in git from the start, in
the same commit that introduces them (or in the next commit). Do not leave
them untracked on the author's local machine.

### Rule

Any new file in `mockups/`, `designs/`, or any other artifact directory should
be `git add`ed alongside the work it informs. Naming convention for superseded
mockups: prefix with `DEPRECATED-` and include the date, e.g.
`mockups/DEPRECATED-mockup-driver-feature-v1-2026-05-07-AM.html`. Keep the
deprecated file in git if it documents a meaningfully different design
direction (useful for future archaeology); delete it if it adds zero unique
reference value over the successor.

### Why

Claude Code worktrees and future engineers can only see files that are tracked
in git. An untracked mockup on the author's local checkout is invisible to
anyone else — including isolated worktree sessions that are doing the
implementation work the mockup is meant to inform.

The driver feature work in May 2026 hit this friction directly: commit
`de9f06e` was a one-off mid-stream `git add` of a single mockup file because
the Claude Code worktree implementing the feature couldn't see it otherwise.
That should be the default, not the exception.

### Scope

This rule applies only to **design artifacts** — mockups, screenshots, design
exports, visual reference material. It does not apply to operational notes,
working scratch files, audit memos, or other personal working artifacts. Those
remain at the author's discretion to track or not.
