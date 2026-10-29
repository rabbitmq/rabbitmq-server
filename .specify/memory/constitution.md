<!--
Sync Impact Report
Version change: (blank template) → 1.0.0
Modified principles: none (initial adoption)
Added sections: Core Principles (I-III), Scope of Testing, Development Workflow, Governance
Removed sections: unused template principle slots 4 and 5
Follow-up TODOs: none
-->
# RabbitMQ Server Constitution

## Core Principles

### I. Public API Compatibility

Public APIs MUST remain backwards compatible. This covers protocol behaviour, the HTTP API,
CLI commands and their output, configuration keys, and exported functions of modules
published as libraries.

A breaking change is allowed only when the user has explicitly instructed it. In that case the
breaking change MUST be called out in the spec, the plan and the commit message.

Rationale: RabbitMQ is deployed in long-lived clusters and used by many client libraries.
Upgrades must not require client or operator changes unless that was the intent.

### II. Targeted Testing

Tests MUST be run only for the code that was changed: the affected suite, group or test case,
as described in `CONTRIBUTING.md`. The entire test suite takes very long to run and MUST be
left to CI.

Rationale: targeted runs give fast feedback, and CI is the place for full coverage.

### III. Adhere to CLAUDE.md

All work MUST follow the guidelines in `CLAUDE.md`, including the rules on comments, voice,
commit messages and pull request descriptions, backporting, and the iterative review after
completing a task.

When this constitution and `CLAUDE.md` appear to conflict, the stricter rule applies.

Rationale: `CLAUDE.md` is the maintainers' checked-in source of repository conventions.

## Scope of Testing

 * Consult `CONTRIBUTING.md` before running any tests
 * Run a single suite, group or case in the affected `deps/` component
 * Use `gmake dialyze` and `gmake xref` from the affected component directory
 * Do not run the full test suite of a component or of the repository locally

## Development Workflow

 * Target the `main` branch unless told otherwise
 * Backport with `git cherry-pick -x`
 * Never add co-author trailers or any mention of AI tooling to commits
 * After a task, perform the iterative reviews described in `CLAUDE.md`

## Governance

This constitution takes precedence over default agent behaviour. Amendments are made by
updating this file through `/speckit-constitution`, with the version bumped as follows:

 * MAJOR: a principle is removed or redefined in a backwards incompatible way
 * MINOR: a principle or section is added or materially expanded
 * PATCH: clarifications and wording fixes

Specs, plans and task lists MUST be checked for compliance with the principles above, and any
deviation MUST be justified in the document where it occurs.

**Version**: 1.0.0 | **Ratified**: 2026-10-29 | **Last Amended**: 2026-10-29
