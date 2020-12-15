# Experimental Bazel build

`brew install bazelisk`

## Running tests

First, create a `.bazelrc` file with at least:

```
build --//bazel_erlang:erlang_home=/path/to/erlang/installation
```

### Run all tests

`bazel test //...`

### Run tests in a 'package' and its 'subpackages'

`bazel test //deps/rabbit_common/...`

### Run tests for a specific 'package'

`bazel test deps/rabbit:all`
