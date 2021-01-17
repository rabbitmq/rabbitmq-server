# Experimental Bazel build

`brew install bazelisk`

Additionally, create a `.bazelrc` file with at least:

```
build --//bazel_erlang:erlang_version=ERLANG_VERSION
build --//bazel_erlang:erlang_home=/path/to/erlang/installation
build --//bazel_erlang:elixir_home=/path/to/elixir/installation
build --//bazel_erlang:mix_archives=~/.mix/archives
```

Your `mix_archives` is likely different than above if you have used kiex for elixir.

## Run the broker

`bazel run :broker`

## Running tests

### Run all tests

`bazel test //...`

### Run tests in a 'package' and its 'subpackages'

`bazel test //deps/rabbit_common/...`

### Run tests for a specific 'package'

`bazel test deps/rabbit:all`
