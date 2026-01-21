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


## GitHub Actions

This repository uses GitHub Actions for CI and releases. Find them at their usual place in `.github/workflows`
and [on the Web](https://github.com/rabbitmq/rabbitmq-server/actions/).

Jobs and run results can be inspected via `gh` on the command line.

### Release Infrastructure

 * [`rabbitmq/server-packages`](https://github.com/rabbitmq/server-packages) contains workflows for producing open source RabbitMQ releases
 * [`rabbitmq/build-env-images`](https://github.com/rabbitmq/build-env-images) contains OCI build environment images


## Comments

 * Only add very important comments, both in tests and in the implementation
 * Keep comments concise and to the point
 * Add comments above the line they are referring to, not at the end of the line (an example of what's not to do: `1 + 1. %% equals 2`)
 * Make sure to use proper English grammar, in particular articles, punctuation and full stops at the end of sentences except for Markdown list items


## Git and GitHub (sans Actions) Instructions

### General

 * Never add yourself to the list of commit co-authors
 * Never mention yourself in commit messages in any way (no "Generated by", no AI tool links, etc)

### Branches

The currently developed branches are:

 * `main` (becomes 4.3.0)
 * `v4.2.x`
 * `v4.1.x`

### Backporting

When backporting commits to older branches,
always use `git cherry-pick -x` to include a reference to the original commit.

### Fetching GitHub PRs

When fetching a GitHub pull request details or diffs, prefer the Web option over the `gh` CLI tool.
`gh` can require an explicit operation approval.


## Writing Style Guide

 * Never add full stops to Markdown list items


## After Completing a Task

### Iterative Reviews

After completing a task, perform up to twenty iterative reviews of your changes.
In every iteration, look for meaningful improvements that were missed, for gaps in test coverage, and for deviations from the instructions in this file.

If no meaningful improvements are found for three iterations in a row, report it and stop iterating.
