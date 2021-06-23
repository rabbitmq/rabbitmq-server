BAZEL ?= /usr/local/bin/bazel
ifeq (darwin,$(PLATFORM))
$(BAZEL):
	brew install bazel
else
	$(error Install bazel for your platform: https://docs.bazel.build/versions/4.1.0/install.html)
endif

bazel-test: $(BAZEL)
ifeq ($(DEP),)
	$(error DEP must be set to the dependency that this test is for, e.g. deps/rabbit)
endif
ifeq ($(SUITE),)
	$(error SUITE must be set to the ct suite to run, e.g. queue_type if DEP=deps/rabbit)
endif
	$(BAZEL) test //deps/$(notdir $(DEP)):$(SUITE)_SUITE --config=ci
