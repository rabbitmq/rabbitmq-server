BAZELISK ?= /usr/local/bin/bazelisk
ifeq (darwin,$(PLATFORM))
$(BAZELISK):
	brew install bazelisk
else
	$(error Install bazelisk for your platform: https://github.com/bazelbuild/bazelisk)
endif

define USER_BAZELRC
build --@bazel-erlang//:erlang_home=$(shell dirname $$(dirname $$(which erl)))
build --@bazel-erlang//:erlang_version=$(shell erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])), io:fwrite(Version), halt().' -noshell)
build --//:elixir_home=$(shell dirname $$(dirname $$(which iex)))/lib/elixir

# rabbitmqctl wait shells out to 'ps', which is broken in the bazel macOS
# sandbox (https://github.com/bazelbuild/bazel/issues/7448)
# adding "--spawn_strategy=local" to the invocation is a workaround
build --spawn_strategy=local

build --incompatible_strict_action_env

# run one test at a time on the local machine
build --test_strategy=exclusive

# don't re-run flakes automatically on the local machine
build --flaky_test_attempts=1

build:buildbuddy --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
endef

export USER_BAZELRC
user.bazelrc:
	echo "$$USER_BAZELRC" > $@

bazel-test: $(BAZELISK) | user.bazelrc
ifeq ($(DEP),)
	$(error DEP must be set to the dependency that this test is for, e.g. deps/rabbit)
endif
ifeq ($(SUITE),)
	$(error SUITE must be set to the ct suite to run, e.g. queue_type if DEP=deps/rabbit)
endif
	$(BAZELISK) test //deps/$(notdir $(DEP)):$(SUITE)_SUITE
