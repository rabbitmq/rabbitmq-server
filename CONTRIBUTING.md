## Overview

RabbitMQ projects use pull requests to discuss, collaborate on and accept code contributions.
Pull requests is the primary place of discussing code changes.

## How to Contribute

The process is fairly standard:

 * Present your idea to the RabbitMQ core team using [GitHub Discussions](https://github.com/rabbitmq/rabbitmq-server/discussions) or [RabbitMQ community Discord server](https://rabbitmq.com/discord)
 * Fork the repository or repositories you plan on contributing to
 * Run `git clean -xfffd && gmake clean && gmake distclean && gmake` to build all subprojects from scratch
 * Create a branch with a descriptive name
 * Make your changes, run tests, ensure correct code formatting, commit with a [descriptive message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html), push to your fork
 * Submit pull requests with an explanation what has been changed and **why**
 * Submit a filled out and signed [Contributor Agreement](https://cla.pivotal.io/) if needed (see below)
 * Be patient. We will get to your pull request eventually


## Running Tests

Test suites of individual subprojects can be run from the subproject directory under
`deps/*`. For example, for the core broker:

``` shell
# Running all server suites in parallel will take between 30 and 40 minutes on reasonably
# recent multi-core machines. This is rarely necessary in development environments.
# Running individual test suites or groups of test suites can be enough.
#

# Before you start: this will terminate all running nodes, make processes and Common Test processes
killall -9 beam.smp; killall -9 erl; killall -9 make; killall -9 epmd; killall -9 erl_setup_child; killall -9 ct_run

# the core broker subproject
cd deps/rabbit

# cleans build artifacts
git clean -xfffd
gmake clean; gmake distclean

# builds the broker and all of its dependencies
gmake
# runs an integration test suite, tests/rabbit_fifo_SUITE with CT (Common Test)
gmake ct-rabbit_fifo
# runs an integration test suite, tests/quorum_queue_SUITE with CT (Common Test)
gmake ct-quorum_queue
# runs an integration test suite, tests/queue_parallel_SUITE with CT (Common Test)
gmake ct-queue_parallel
# runs a unit test suite tests/unit_log_management_SUITE with CT (Common Test)
gmake ct-unit_log_management
```

### Running Specific Groups or Tests

All `ct-*` Make targets support a `t=` argument which are transformed to [`-group` and `-case` Common Test runner options](https://www.erlang.org/doc/apps/common_test/run_test_chapter.html).

``` shell
# Runs a a group of tests named 'all_tests_with_prefix' in suite 'test/rabbit_mgmt_http_SUITE.erl'
gmake ct-rabbit_mgmt_http t="all_tests_with_prefix"

# Runs a test named 'users_test' in group 'all_tests_with_prefix' in suite 'test/rabbit_mgmt_http_SUITE.erl'
gmake ct-rabbit_mgmt_http t="all_tests_with_prefix:users_test"
# Runs a test named 'queues_test' in group 'all_tests_with_prefix' in suite 'test/rabbit_mgmt_http_SUITE.erl'
gmake ct-rabbit_mgmt_http t="all_tests_with_prefix:queues_test"
```

### Running Tests with a Specific Schema Data Store

Set `RABBITMQ_METADATA_STORE` to either `khepri` or `mnesia` to make the Common Test suites
use a specific [schema data store]() (metadata store):

``` shell
RABBITMQ_METADATA_STORE=khepri gmake ct-quorum_queue
```

Or, with Nu shell:

```nu
with-env {'RABBITMQ_METADATA_STORE': 'khepri'} { gmake ct-quorum_queue }
```

### Running Mixed Version Tests

For some components, it's important to run tests in a mixed-version cluster, to make sure the upgrades
are handled correctly. For example, you may want to make sure that the quorum_queue suite passes, when
there's a mix of RabbitMQ 4.1 and 4.2 nodes in the cluster.

Here's how you can do that:

```shell
# download the older version, eg:
https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.1.1/rabbitmq-server-generic-unix-4.1.1.tar.xz

# unpack it
tar xf rabbitmq-server-generic-unix-4.1.1.tar.xz

# run the test with SECONDARY_DIST pointing at the extracted folder
SECONDARY_DIST=rabbitmq_server-4.1.1 make -C deps/rabbit ct-quorum_queue
```

Odd-numbered nodes (eg. 1 and 3) will be started using the main repository, while even-numbered nodes (eg. node 2)
will run the older version.

## Running Single Nodes from Source

``` shell
# Run from repository root.
# Starts a node with the management plugin enabled
gmake run-broker RABBITMQ_PLUGINS=rabbitmq_management
```

The nodes will be started in the background. They will use `rabbit@{hostname}` for its name, so CLI will be able to contact
it without an explicit `-n` (`--node`) argument:

```shell
# Run from repository root.
./sbin/rabbitmq-diagnostics status
```

## Running Clusters from Source

``` shell
# Run from repository root.
# Starts a three node cluster with the management plugin enabled
gmake start-cluster NODES=3 RABBITMQ_PLUGINS=rabbitmq_management
```

The node will use `rabbit-{n}@{hostname}` for names, so CLI must
be explicitly given explicit an `-n` (`--node`) argument in order to
contact one of the nodes:

 * `rabbit-1`
 * `rabbit-2`
 * `rabbit-3`

The names of the nodes can be looked up via

``` shell
epmd -names
```

``` shell
# Run from repository root.
# Makes CLI tools talk to node rabbit-2
sbin/rabbitmq-diagnostics cluster_status -n rabbit-2

# Run from repository root.
# Makes CLI tools talk to node rabbit-1
sbin/rabbitmq-diagnostics status -n rabbit-1
```

To stop a previously started cluster:

``` shell
# Run from repository root.
# Stops a three node cluster started earlier
gmake stop-cluster NODES=3
```


## Working on Management UI with BrowserSync

When working on management UI code, besides starting the node with

``` shell
# starts a node with the management plugin enabled
gmake run-broker RABBITMQ_PLUGINS=rabbitmq_management
```

(or any other set of plugins), it is highly recommended to use [BrowserSync](https://browsersync.io/#install)
to shorten the edit/feedback cycle for JS files, CSS, and so on.

First, install BrowserSync using NPM:

``` shell
npm install -g browser-sync
```

Assuming a node running locally with HTTP API on port `15672`, start
a BrowserSync proxy like so:

``` shell
cd deps/rabbitmq_management/priv/www

browser-sync start --proxy localhost:15672 --serverStatic . --files .
```

BrowserSync will automatically open a browser window for you to use. The window
will automatically refresh when one of the static (templates, JS, CSS) files change.

All HTTP requests that BrowserSync does not know how to handle will be proxied to
the HTTP API at `localhost:15672`.


## Formatting the RabbitMQ CLI

The RabbitMQ CLI uses the standard [Elixir code formatter](https://hexdocs.pm/mix/main/Mix.Tasks.Format.html). To ensure correct code formatting of the CLI:

```
cd deps/rabbitmq_cli
mix format
```

Running `make` will validate the CLI formatting and issue any necessary warnings. Alternatively, run the format checker in the `deps/rabbitmq_cli` directory:

```
mix format --check-formatted
```

## Code of Conduct

See [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

## Contributor Agreement

If you want to contribute a non-trivial change, please submit a signed copy of our
[Contributor Agreement](https://cla.pivotal.io/) around the time
you submit your pull request. This will make it much easier (in some cases, possible)
for the RabbitMQ team at Pivotal to merge your contribution.

## Where to Ask Questions

If something isn't clear, feel free to ask on [GitHub Discussions](https://github.com/rabbitmq/rabbitmq-server/discussions)
and [community Discord server](https://rabbitmq.com/discord).
