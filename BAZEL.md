# [Bazel](https://www.bazel.build/) build

From https://docs.bazel.build/versions/master/bazel-overview.html
> Bazel is an open-source build and test tool similar to Make, Maven, and Gradle. It uses a human-readable, high-level build language. Bazel supports projects in multiple languages and builds outputs for multiple platforms. Bazel supports large codebases across multiple repositories, and large numbers of users.

## Why RabbitMQ + Bazel?

RabbitMQ, Tier1 plugins included, is a large codebase. The developer experience benefits from fast incremental compilation.

More importantly, RabbitMQ's test suite is large and takes hours if run on a single machine. Bazel allows tests to be run in parallel on a large number of remote workers if needed, and furthermore uses cached test results when branches of the codebase remain unchanged.

Bazel does not provide built in Erlang or Elixir support, nor is there an available library of bazel rules. Therefore, we have defined our own rules in https://github.com/rabbitmq/bazel-erlang. Elixir compilation is handled as a special case within this repository. To use these rules, the location of your Erlang and Elixir installations must be indicated to the build (see below).

While most of work for running tests happens in Bazel, the suite still makes use of some external tools for commands, notably gnu `make` and `openssl`. Ideally we could bring all of these tools under bazel, so that the only tool needed would be `bazel` or `bazelisk`, but that will take some time.

## Running Tests

### Install Bazelisk

On **macOS**:

`brew install bazelisk`

Otherwise:

https://docs.bazel.build/versions/master/install-bazelisk.html

### Create `user.bazelrc`

Create a `user.bazelrc` by making a copy of `user-template.bazelrc` and updating the paths in the first few lines.

### Run the broker

`bazel run broker`

You can set different environment variables to control some configuration aspects, like this:

```
    RABBITMQ_CONFIG_FILES=/path/to/conf.d \
    RABBITMQ_NODENAME=<node>@localhost \
    RABBITMQ_NODE_PORT=7000 \
    bazel run broker
```

This will start RabbitMQ with configs being read from the provided directory. It also will start a node with a given node name, and with all listening ports calculated from the given one - this way you can start non-conflicting rabbits even from different checkouts on a single machine.


### Running tests

Many rabbit tests spawn single or clustered rabbit nodes, and therefore it's best to run test suites sequentially on a single machine. Hence the `build --local_test_jobs=1` flag used in `.bazelrc`. Additionally, it may be reasonable to disable test sharding and stream test output when running tests locally with `--test_output=streamed` as an additional argument (to just disable sharding, but not stream output, use `--test_sharding_strategy=disabled`). Naturally that restriction does not hold if utilizing remote execution (as is the case for RabbitMQ's CI pipelines).

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

## Add/update an external dependency from hex.pm

1. `bazel run gazelle-update-repos -- -args hex.pm/accept@0.3.5` to generate/update `bazel/BUILD.accept`
1. `git checkout WORKSPACE` to reset the workspace file
1. Add/update the entry in MODULE.bazel

## Add/update an external dependency from github

`bazel run gazelle-update-repos -- -args --testonly github.com/extend/ct_helper`

## Additional Useful Commands

- Format all bazel files consistently (requires [buildifier](https://github.com/bazelbuild/buildtools/blob/master/buildifier/README.md)):

  `buildifier -r .`

- Remove unused load statements from BUILD.bazel files (requires [buildozer](https://github.com/bazelbuild/buildtools/blob/master/buildozer/README.md)):

  `buildozer 'fix unusedLoads' //...:__pkg__`
