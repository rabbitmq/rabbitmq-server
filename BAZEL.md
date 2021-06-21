# Experimental [Bazel](https://www.bazel.build/) build

From https://docs.bazel.build/versions/master/bazel-overview.html
> Bazel is an open-source build and test tool similar to Make, Maven, and Gradle. It uses a human-readable, high-level build language. Bazel supports projects in multiple languages and builds outputs for multiple platforms. Bazel supports large codebases across multiple repositories, and large numbers of users.

## Why RabbitMQ + Bazel?

RabbitMQ, Tier1 plugins included, is a large codebase. The developer experience benefits from fast incremental compilation.

More importantly, RabbitMQ's test suite is large and takes hours if run on a single machine. Bazel allows tests to be run in parallel on a large number of remote workers if needed, and furthermore uses cached test results when branches of the codebase remain unchanged.

To the best of our knowledge Bazel does not provide built in Erlang or Elixir support, nor is there an available library of bazel rules. Therefore, we have defined our own rules in https://github.com/rabbitmq/bazel-erlang. Elixir compilation is handled as a special case within this repository. To use these rules, the location of your Erlang and Elixir installations must be indicated to the build (see below).

While most of work for running tests happens in Bazel, the suite still makes use of some external tools for commands, notably gnu `make` and `openssl`. Ideally we could bring all of these tools under bazel, so that the only tool needed would be `bazel` or `bazelisk`, but that will take some time.

## Install Bazelisk

On **macOS**:

`brew install bazelisk`

Otherwise:

https://docs.bazel.build/versions/master/install-bazelisk.html

Additionally, create a `.bazelrc` file with at least:

```
build --@bazel-erlang//:erlang_home=/path/to/erlang/installation
build --@bazel-erlang//:erlang_version=23.1
build --@bazel-erlang//:elixir_home=/path/to/elixir/installation
build --test_strategy=exclusive
build --incompatible_strict_action_env
```

Additionally, on **macOS**, you will likely need to add

```
build --spawn_strategy=local
```

for certain `rabbitmq_cli` tests to pass. This is because `rabbitmqctl wait` shells out to 'ps', which is broken in the bazel macOS (https://github.com/bazelbuild/bazel/issues/7448).

## Run the broker

`bazel run broker`

## Running tests

Many rabbit tests spawn single or clustered rabbit nodes, and therefore it's best to run test suites sequentially on a single machine. Hence the `--test_strategy=exclusive` flag used in `.bazelrc` above. Naturally that restriction does not hold if utilizing remote execution (as is the case for RabbitMQ's CI pipelines).

Erlang Common Test logs will not be placed in the logs directory when run with bazel. They can be found under `bazel-testlogs`. For instance, those of the rabbit application's backing_queue suite will be under `bazel-testlogs/deps/rabbit/backing_queue_SUITE/test.outputs/`.

### Run all tests

Note: This takes quite some time on a single machine.

`bazel test //...`

### Run tests in a 'package' and its 'subpackages'

**rabbit** is an appropriate example because it encloses the **rabbitmq_prelaunch** application.

`bazel test deps/rabbit/...`

### Run tests for a specific 'package'

`bazel test deps/rabbit_common:all`

### Run an individual common test suite

`bazel test //deps/rabbit:lazy_queue_SUITE`
