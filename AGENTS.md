# Instructions for AI Agents

## Overview

This repository contains open source [RabbitMQ](https://www.rabbitmq.com/), a multi-protocol
messaging and streaming broker that supports AMQP 1.0, AMQP 0-9-1, MQTTv5, the [RabbitMQ Stream Protocol](https://www.rabbitmq.com/docs/streams), STOMP 1.2,
MQTT-over-WebSockets, and STOMP-over-WebSockets.


## Website and GitHub Repositories

To learn more about RabbitMQ and its features, visit [rabbitmq.com](https://www.rabbitmq.com/).

The mainline repository on GitHub is [`rabbitmq/rabbitmq-server`](https://github.com/rabbitmq/rabbitmq-server/),
the website repository is [`rabbitmq/rabbitmq-website`](https://github.com/rabbitmq/rabbitmq-website/).


## Building and Testing

The GNU Make 4-based build system is described in `CONTRIBUTING.md`.

Consult `CONTRIBUTING.md` before running any tests, in particular to learn how to run
a specific suite, group of cases or a single test case.

When looking for GNU Make 4, consult `gmake` as well as `make`.

### Dialyzer and xref

Use `gmake dialyze` and `gmake xref` to run static code analysis tools
from individual `deps/` component directories (see below).


## Repository Structure

 * `deps/rabbit`: the core RabbitMQ server, the most important part of the codebase
 * `deps/rabbit_common`: internal library for common modules
 * `deps/rabbitmq_amqp1_0`: a no-op plugin that exists for backwards compatibility since AMQP 1.0 is a core protocol as of RabbitMQ 4.0
 * `deps/rabbitmq_amqp_client`: Erlang AMQP 1.0 client with RabbitMQ-specific management operations
 * `deps/rabbitmq_auth_backend_http`: external HTTP server-based authentication (authN), authorization (authZ) backend
 * `deps/rabbitmq_auth_backend_internal_loopback`: a `localhost`-only version of the internal authN, authZ backend
 * `deps/rabbitmq_auth_backend_ldap`: LDAP authN, authZ plugin
 * `deps/rabbitmq_auth_backend_oauth2`: OAuth 2.0 authN, authZ backend
 * `deps/rabbitmq_auth_backend_cache`: a caching layer for other authN, authZ backends
 * `deps/rabbitmq_auth_mechanism_ssl`: X.509 certificate-based authentication support
 * `deps/rabbitmq_aws`: AWS API client library
 * `deps/rabbitmq_cli`: standard CLI tools (`rabbitmqctl`, `rabbitmq-plugins`, `rabbitmq-diagnostics`, etc.); note that [`rabbitmqadmin` v2](https://www.rabbitmq.com/docs/management-cli) lives in a separate repository, [`rabbitmq/rabbitmqadmin-ng`](https://github.com/rabbitmq/rabbitmqadmin-ng)
 * `deps/rabbitmq_codegen`: generates AMQP 0-9-1 serialization modules from machine-readable specification documents
 * `deps/rabbitmq_consistent_hash_exchange`: consistent hashing exchange (`x-consistent-hash`)
 * `deps/rabbitmq_ct_client_helpers`: Common Test helpers for managing connections, channels
 * `deps/rabbitmq_ct_helpers`: Common Test helpers used by RabbitMQ test suites
 * `deps/rabbitmq_event_exchange`: exposes internal events to AMQP 0-9-1 clients
 * `deps/rabbitmq_exchange_federation`: exchange federation
 * `deps/rabbitmq_queue_federation`: queue federation
 * `deps/rabbitmq_federation`: a no-op plugin that depends on `rabbitmq_queue_federation` and `rabbitmq_exchange_federation`
 * `deps/rabbitmq_federation_common`: a common library used by federation plugins
 * `deps/rabbitmq_federation_management`: management UI extension for federation
 * `deps/rabbitmq_federation_prometheus`: Prometheus metrics for federation
 * `deps/rabbitmq_jms_topic_exchange`: JMS topic exchange (`x-jms-topic`) with SQL selection rules
 * `deps/rabbitmq_management`: management plugin, including the HTTP API and management UI code
 * `deps/rabbitmq_management/priv/www`: management UI code
 * `deps/rabbitmq_management_agent`: collects node-wide metrics reported by the management plugin
 * `deps/rabbitmq_mqtt`: MQTT protocol support
 * `deps/rabbitmq_peer_discovery_aws`: AWS EC2-based peer discovery
 * `deps/rabbitmq_peer_discovery_common`: common library for peer discovery backends
 * `deps/rabbitmq_peer_discovery_consul`: Consul-based peer discovery
 * `deps/rabbitmq_peer_discovery_etcd`: etcd-based peer discovery (v3 API)
 * `deps/rabbitmq_peer_discovery_k8s`: Kubernetes peer discovery
 * `deps/rabbitmq_prelaunch`: internal component used very early on node boot
 * `deps/rabbitmq_prometheus`: Prometheus plugin
 * `deps/rabbitmq_random_exchange`: random exchange (`x-random`)
 * `deps/rabbitmq_recent_history_exchange`: recent history exchange (`x-recent-history`)
 * `deps/rabbitmq_sharding`: an opinionated exchange plugin that's lost relevance in the age of [super streams](https://www.rabbitmq.com/docs/streams)
 * `deps/rabbitmq_shovel`: the shovel plugin
 * `deps/rabbitmq_shovel_management`: management UI extension for shovel
 * `deps/rabbitmq_shovel_prometheus`: Prometheus metrics for shovel
 * `deps/rabbitmq_stomp`: STOMP protocol support
 * `deps/rabbitmq_stream`: the streaming subsystem and a RabbitMQ Stream Protocol implementation
 * `deps/rabbitmq_stream_common`: common library for streams
 * `deps/rabbitmq_stream_management`: management UI extension for streams
 * `deps/rabbitmq_top`: `top`-like Erlang runtime process viewer
 * `deps/rabbitmq_tracing`: a plugin that traces messages
 * `deps/rabbitmq_trust_store`: an opinionated alternative to traditional TLS peer verification
 * `deps/rabbitmq_web_dispatch`: a shared foundation for all HTTP- and WebSocket-based plugins
 * `deps/rabbitmq_web_mqtt`: MQTT-over-WebSockets
 * `deps/rabbitmq_web_mqtt_examples`: MQTT-over-WebSockets examples (with a Web UI part)
 * `deps/rabbitmq_web_stomp`: STOMP-over-WebSockets
 * `deps/rabbitmq_web_stomp_examples`: STOMP-over-WebSockets examples (with a Web UI part)
 * `docs/compatibility.json`: machine-readable Erlang/Elixir compatibility matrix for all releases from 3.11.0 onwards. See `docs/COMPATIBILITY.md` for maintenance instructions
 * `scripts` contains shell scripts that drive the server and CLI tools
 * `packaging` contains *some* packaging-related code; release artifacts source can be found in [`rabbitmq/rabbitmq-packaging`](https://github.com/rabbitmq/rabbitmq-packaging)
 * `selenium` contains Selenium tests for the management UI and the OAuth 2 plugin
 * `release-notes` contains release notes all the way back to 1.0.0 previews


## Key Dependencies

Dependency sources, repositories, and versions are defined in `rabbitmq-components.mk`.

These dependencies are cloned by `gmake` during the build process:

 * `deps/ranch` is [Ranch](https://github.com/ninenines/ranch), a socket acceptor library used by all protocol implementations
 * `deps/ra` is [Ra](https://github.com/rabbitmq/ra), our [Raft](https://raft.github.io/) implementation
 * `deps/aten` is [`aten`](https://github.com/rabbitmq/aten), an implementation of [adaptive accrual failure detector](https://dl.acm.org/doi/10.1145/1244002.1244129) for Ra
 * `deps/osiris` is [`osiris`](https://github.com/rabbitmq/osiris), a library that underpins the streaming subsystem
 * `deps/khepri` is [`khepri`](https://github.com/rabbitmq/khepri), an embedded distributed Ra-based [schema data store](https://www.rabbitmq.com/docs/metadata-store)
 * `deps/cuttlefish` is [`cuttlefish`](https://github.com/Kyorai/cuttlefish/), a `rabbitmq.conf` parser and translation library
 * `deps/cowboy` is the HTTP server and API framework used by the RabbitMQ HTTP API and other HTTP and WebSockets-based plugins
 * `deps/thoas` is [Thoas](https://github.com/lpil/thoas), a JSON parser and generator
 * `deps/seshat` is [`seshat`](https://github.com/rabbitmq/seshat), a counters (metrics) library


## Build System Files, Build Artifacts, Test Run Logs

 * `erlang.mk` is the heart of the Make-based build system
 * `rabbitmq-components.mk` lists all dependencies, their sources (e.g. a Git repo or `hex.pm`) and target version
 * `mk`, `./*.mk`, `deps/rabbit_common/mk` are various Make files included into `Makefile`
 * `ebin`, `sbin`, `escript`, `plugins` directories contain build artifacts
 * `logs` contains Common Test run logs. Inspect it when troubleshooting test failures
 * `rebar.config`: Rebar configuration; Rebar is used sparingly throughout the codebase; Make is the primary build tool


## Target Erlang and Elixir Versions

RabbitMQ [targets Erlang `27.x`](https://www.rabbitmq.com/docs/which-erlang) and a reasonably [recent Elixir](https://github.com/elixir-lang/elixir/releases) (e.g. `1.18.x`, `1.19.x`).

Per-release Erlang and Elixir compatibility ranges in machine-readable format
can be found in `docs/compatibility.json`.


## GitHub Actions

This repository uses GitHub Actions for CI and releases. Find them at their usual place in `.github/workflows`
and [on the Web](https://github.com/rabbitmq/rabbitmq-server/actions/).

Jobs and run results can be inspected via `gh` on the command line.

### Release Infrastructure

 * [`rabbitmq/server-packages`](https://github.com/rabbitmq/server-packages) contains workflows for producing open source RabbitMQ releases
 * [`rabbitmq/build-env-images`](https://github.com/rabbitmq/build-env-images) contains OCI build environment images

 
## Comments

The default number of new comments in a change is zero.

Maintainers routinely spend follow-up commits shortening or deleting comments that
came with a contribution. A comment that has to be edited or removed later costs more
than the missing comment would have.

### When to Comment

Add a comment only when a reader who knows Erlang and the module would get something
wrong without it. In practice, that leaves a short list:

 * A non-obvious reason: backwards compatibility with persisted data or older nodes, a
   workaround for a specific Erlang/OTP version, a known deviation from a protocol spec
 * A property that is easy to break by accident: atom table exhaustion, a timing side
   channel, a value that clients control and therefore cannot be trusted
 * A reference that the code cannot carry: an issue number, a commit, an RFC section

Everything else goes without a comment. In particular:

 * Anything the code, the function name, or the test case name already says
 * Narration of the change: "now", "previously", "used to", "with the fix", "the
   headline scenario". This belongs in the commit message
 * The steps of a test: "Baseline", "Set up", "Clean up the queue", "Give it a chance
   to happen"
 * Erlang, OTP or Common Test basics
 * A rule that a guard, a pattern, a type spec or a function boundary can enforce.
   Enforce it in code instead
 * Justification for the comment itself, or reassurance about what callers "must
   never" do

When in doubt, leave it out: a reviewer can always ask for a missing comment.

### Form

 * One line is the norm, two or three is the ceiling. Longer is reserved for truly
   complex invariants, and then split into short paragraphs with a blank `%%` line
 * Present tense, neutral tone. State the why, not the what
 * No intensifiers ("huge", "massive", "extremely"). Name the actual condition or limit instead
 * Use the terms the module and the protocol spec already use. Do not coin new ones
   ("ledger", "budget", "poisoned", "hijack") or introduce new abbreviations
 * Wrap identifiers in backticks. A function reference always includes its arity:
   `supervisor:which_children/1`, or `handle_info/2` for a function in the same module.
   Never a bare `which_children` or `which_children()`
 * A comment goes on its own line above the code it refers to, never at the end of a line
   (not `1 + 1. %% equals 2`)
 * Use proper English grammar: articles, punctuation, and full stops at the end of sentences
 * Do not add comments to code the change does not otherwise touch

### Examples

A comment that earns its place. It explains a constraint that the code cannot show:

```erlang
%% Do not change this value in place. The iteration count is not stored
%% with the hash, so every existing PBKDF2 user would be locked out.
%% Introduce a new module with the new value instead.
-define(ITERATIONS, 210_000).
```

The pairs below are taken from commits that edited contributed comments.

Before:

```erlang
%% parse_deferred_tokens/1 only caps each individual FLOW
%% frame's own batch; since FLOW frames aren't subject to
%% session incoming-window flow control, a client can
%% pipeline many of them while a credit request is in
%% flight and grow the stash unboundedly if the combined
%% length isn't capped here too.
```

After:

```erlang
%% Also cap the combined token count across stashed `FLOW` frames.
```

Before:

```erlang
%% rabbitmqctl set_permissions/clear_permissions don't notify live
%% sessions, so expire the cache on a timer instead.
```

After:

```erlang
%% A permission change does not reach a running shovel, so the cache
%% expires on a timer.
```

Before:

```erlang
%% Marking the node as being drained is the single load-bearing step:
%% it is what makes the rest of the cluster stop routing new work here.
%% It must succeed for the drain to be meaningful, so a failure here
%% is intentionally propagated. Every step that follows is
%% housekeeping around cleaning up in-flight state; a failure in one
%% of them (e.g. a stuck federation link, a plugin drain callback
%% that crashes, a channel that refuses to shut down within the
%% termination timeout) must not abort the drain and must not
%% surface as a non-zero CLI exit code, or automation like
%% Kubernetes preStop hooks becomes unreliable. See GH #3369.
```

After:

```erlang
%% Marking the node as drained stops the cluster routing work here, so
%% a failure is propagated. The rest is best-effort cleanup that must
%% not abort the drain nor fail the CLI command, or automation such as
%% Kubernetes preStop hooks breaks. rabbitmq/rabbitmq-server#3369.
```

Before:

```erlang
  %% Get the link up and running before poking at it.
```

After:

```erlang
  %% Wait for the link to come up first.
```

Before:

```erlang
    %% Check that the content type is json
    %% ...
    %% Decode the JSON body
    %% ...
    %% Check that the settings are present
```

After: all three comments are removed.


## Voice

Write like a senior engineer who values clarity and simplicity. This applies
to all prose: comments, commit messages, pull request descriptions, design docs and notes.

 * Plain and factual: state the why in one line, never narrate the what
 * Literal mechanism over metaphor: name the actual thing, not an image of it
 * Prefer the plainest word. No coined verbs, no jargon, no sophisticated synonym use for its own sake
 * No flourish, no editorializing, no imagery. Use real domain terms
 * Claim only what has been verified: "an attempt to fix a flake" until the fix is confirmed
 * Undersell rather than oversell: "a minor correctness fix", "one more test", never
   "comprehensive" or "significantly improved"
 * Write sentences a person would say out loud, contractions included. If a sentence reads
   like a log line or a status enum, rewrite it

### Writing Style, Markdown Style

 * Never add full stops to Markdown list items
 * Use "X and Y" in prose, not "X / Y" slash-shorthand. Exceptions: unit
   fractions (`bytes/edge`), single-concept abbreviations (`I/O`), and paths
   or code (`tests/unit/`, `m:f/a`, `queue.declare`)
 * Wrap code identifiers — types, functions, modules, file names, paths — and version numbers (`4.3.5`, `27.x`) in backticks in prose
 * Both British and American spelling are in use. Match the spelling the file already uses
 * Avoid robotic labels such as `**Thing / other:**`; write a plain sentence or a simple label
 * Match the existing conventions of the file and subdirectory you are
   editing — bullet character, heading depth, ID schemes, and table shape
   vary by project, and the local choice wins
 * Spell the causation out with a plain connective: "this means that the
   decisions can be tested without a cluster", not the compressed "keeps
   the decisions testable"
 * One idea per paragraph: in multi-line doc strings, separate distinct
   thoughts with a blank line rather than cramming them into one block
 * A short orienting connective is fine where it helps the reader ("as the
   name suggests", "note that"): this is clarity, not flourish
 * Grammar counts, including the passive where it is the correct voice: "a
   command has been committed", not "a command has committed"


## Git and GitHub (sans Actions) Instructions

### General

 * Never add yourself to the list of commit co-authors
 * Never mention yourself in commit messages in any way (no "Generated by", no AI tool links, etc)

### Commit Messages and Pull Request Descriptions

 * Terse, factual, neutral and simple. No jargon. A body is fine when it explains something the diff cannot
 * No full stop at the end of the subject line
 * Imperative mood, a noun phrase, or a plain declarative sentence; never past tense:
   `Fix a flaky test`, `Stream SAC: update another test`, `` `hash_password/1` is now `hash_password/2` ``
 * Lead with a subsystem or module label and a colon when it helps: `LDAP: handle more Active Directory username edge cases`
 * State the how with a verb phrase: `fix a flake by allowing for more time`, not "timeout adjustment for flake mitigation"
 * Reference issues and PRs bare, without parentheses: `References #17255`, `Fixes #11526`
 * A body explains why in a few short paragraphs. It does not list every changed file or function
 * The same applies to pull request descriptions: what the problem was, how it is fixed, how it was tested.
   No headings for a small change, no summary tables, no checklists of self-evident items

### Branches

The currently developed community-supported branches are:

 * `main` (becomes 4.4.0)
 * `v4.3.x`

### Backporting

When backporting commits to older branches,
always use `git cherry-pick -x` to include a reference to the original commit.

### Fetching GitHub PRs

When fetching a GitHub pull request details or diffs, prefer the Web option over the `gh` CLI tool.
`gh` can require an explicit operation approval.


## Security

`.github/SECURITY.md` describes the security policy.

For GitHub Security Advisories, make sure to:

 * Provide a complete CVSS 4.0 vector string
 * Enter the "Ecosystem" and "Package name" correctly
   * For vulnerabilities in packages published on [Hex.pm](https://hex.pm/users/rabbitmq) (e.g. `amqp10_client`, `amqp10_common`, `amqp_client`, `rabbit_common`, `credentials_obfuscation`):
     * Set "Ecosystem" to "Erlang"
     * Set "Package name" to the Hex.pm package name
     * This ensures they are properly picked up by OSV.dev and Hex.pm
   * For vulnerabilities not present in a Hex.pm package (e.g. in `deps/rabbit`):
     * Set "Ecosystem" to "Other: RabbitMQ"
     * Set "Package name" to "rabbitmq-server"
     * This helps the GitHub team differentiate them from Hex.pm packages
 * Enter the "Affected Versions" and "Patch Versions" correctly
   * Define "Affected Versions" explicitly per minor release line using version ranges (e.g. `>= 4.2.0`, `< 4.2.10`)
   * For the Patch version, provide the exact release containing the fix (e.g. `4.2.6`)
   * If a fix is included in the first release of a new minor line (e.g. `4.3.0`), omit that release line entirely as no vulnerable `4.3.x` releases exist

### Use CVSSv4 Scores

When computing a CVSS score, use the CVSSv4 calculator: it can express certain important nuaces better.

When a potential vulnerability is found in a plugin or a feature not enabled by default,
set the `Attack Requirements` (`AT`) metric to `Present` (`P`): `AT:P`, to reflect the fact
that only a subset of deployments are affected.


## After Completing a Task

### Iterative Reviews

After completing a task, perform up to twenty iterative reviews of your changes.
In every iteration, look for meaningful improvements that were missed, for gaps in test coverage, and for deviations from the instructions in this file,
including new comments that can be shortened or dropped.

If no meaningful improvements are found for three iterations in a row, report it and stop iterating.
