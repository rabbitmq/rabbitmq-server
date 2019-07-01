.PHONY: ct-slow ct-fast

ct-slow ct-fast:
	$(MAKE) ct CT_SUITES='$(CT_SUITES)'

# --------------------------------------------------------------------
# xref
# --------------------------------------------------------------------

# We need the list of dependencies of the current project. We use it in
# xrefr(1) to scan for Elixir-based projects. For those, we need to add
# the path inside `_build` to the xref code path.

ifneq ($(filter xref,$(MAKECMDGOALS)),)
export ERLANG_MK_RECURSIVE_DEPS_LIST
endif

xref: $(ERLANG_MK_RECURSIVE_DEPS_LIST)

# --------------------------------------------------------------------
# Helpers to run Make targets on Concourse.
# --------------------------------------------------------------------

FLY ?= fly
FLY_TARGET ?= $(shell $(FLY) targets | awk '/ci\.rabbitmq\.com/ { print $$1; }')

CONCOURSE_TASK = $(ERLANG_MK_TMP)/concourse-task.yaml

CI_DIR ?= $(DEPS_DIR)/ci
PIPELINE_DIR = $(CI_DIR)/server-release
BRANCH_RELEASE = $(shell "$(PIPELINE_DIR)/scripts/map-branch-to-release.sh" "$(base_rmq_ref)")
PIPELINE_DATA = $(PIPELINE_DIR)/release-data-$(BRANCH_RELEASE).yaml
REPOSITORY_NAME = $(shell "$(PIPELINE_DIR)/scripts/map-erlang-app-and-repository-name.sh" "$(PIPELINE_DATA)" "$(PROJECT)")

CONCOURSE_PLATFORM ?= linux
ERLANG_VERSION ?= $(shell "$(PIPELINE_DIR)/scripts/list-erlang-versions.sh" "$(PIPELINE_DATA)" | head -n 1)
TASK_INPUTS = $(shell "$(PIPELINE_DIR)/scripts/list-task-inputs.sh" "$(CONCOURSE_TASK)")

.PHONY: $(CONCOURSE_TASK)
$(CONCOURSE_TASK): $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST)
	$(gen_verbose) echo 'platform: $(CONCOURSE_PLATFORM)' > "$@"
	$(verbose) echo 'inputs:' >> "$@"
	$(verbose) echo '  - name: $(PROJECT)' >> "$@"
	$(verbose) cat $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) | while read -r file; do \
		echo "  - name: $$(basename "$$file")" >> "$@"; \
		done
	$(verbose) echo 'outputs:' >> "$@"
	$(verbose) echo '  - name: test-output' >> "$@"
ifeq ($(CONCOURSE_PLATFORM),linux)
	$(verbose) echo 'image_resource:' >> "$@"
	$(verbose) echo '  type: docker-image' >> "$@"
	$(verbose) echo '  source:' >> "$@"
	$(verbose) echo '    repository: pivotalrabbitmq/rabbitmq-server-buildenv' >> "$@"
	$(verbose) echo '    tag: linux-erlang-$(ERLANG_VERSION)' >> "$@"
endif
	$(verbose) echo 'run:' >> "$@"
	$(verbose) echo '  path: ci/server-release/scripts/test-erlang-app.sh' >> "$@"
	$(verbose) echo '  args:' >> "$@"
	$(verbose) echo "    - $(PROJECT)" >> "$@"
# This section must be the last because the `%-on-concourse` target
# appends other variables.
	$(verbose) echo 'params:' >> "$@"
ifdef V
	$(verbose) echo '  V: "$(V)"' >> "$@"
endif
ifdef t
	$(verbose) echo '  t: "$(t)"' >> "$@"
endif

%-on-concourse: $(CONCOURSE_TASK)
	$(verbose) test -d "$(PIPELINE_DIR)"
	$(verbose) echo '  MAKE_TARGET: "$*"' >> "$(CONCOURSE_TASK)"
	$(FLY) -t $(FLY_TARGET) execute \
		--config="$(CONCOURSE_TASK)" \
		$(foreach input,$(TASK_INPUTS), \
		$(if $(filter $(PROJECT),$(input)), \
		--input="$(input)=.", \
		--input="$(input)=$(DEPS_DIR)/$(input)")) \
		--output="test-output=$(CT_LOGS_DIR)/on-concourse"
	$(verbose) rm -f "$(CT_LOGS_DIR)/on-concourse/filename"
