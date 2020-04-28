.PHONY: github-actions

ifneq ($(filter github-actions,$(MAKECMDGOALS)),)

# All versions of Erlang we want to test against.
ERLANG_VERSIONS ?= 21.3 \
		   22.3

ELIXIR_VERSION ?= 1.8.0

WORKFLOWS_DIR ?= .github/workflows

# One workflow file per Erlang version. The files are in $(WORKFLOWS_DIR).
TESTING_WORKFLOWS := \
	$(foreach erlang,$(ERLANG_VERSIONS),\
	$(WORKFLOWS_DIR)/test-erlang-otp-$(erlang).yaml)

# The directories holding all the pieces used to create a test workflow.
#
# - All pieces of workflow are named *.yaml.
# - Files are sorted alphabetically before generating the workflow.
# - If several directories contain a piece with the same filename, only the
#   first file found will be used. This allows a piece to be overriden.
ifeq ($(PROJECT),rabbit_common)
TESTING_JOBS_DIRS = $(WORKFLOWS_DIR)/test-jobs
else
TESTING_JOBS_DIRS = $(WORKFLOWS_DIR)/test-jobs \
		    $(DEPS_DIR)/rabbit_common/$(WORKFLOWS_DIR)/test-jobs
endif

# This variable takes care of sorting and deduplicating files.
TESTING_JOB_BASENAMES := \
	$(sort \
	$(notdir $(wildcard \
	$(patsubst %,%/*.yaml,$(TESTING_JOBS_DIRS)))))

# This variable takes care of finding the final files which will make the
# workflow. I.e. overriden files are dropped at this point.
TESTING_JOBS := \
	$(foreach job_name,$(TESTING_JOB_BASENAMES),\
	$(firstword $(wildcard $(patsubst %,%/$(job_name),$(TESTING_JOBS_DIRS)))))

# The file named *-CT_SUITE.yaml is special: it will be duplicated for each
# common_test testsuite available in the project.
CT_SUITE_JOB_BASENAME := $(filter %-CT_SUITE.yaml,$(TESTING_JOB_BASENAMES))

WORKFLOWS := $(TESTING_WORKFLOWS)

github-actions: $(WORKFLOWS)
	$(verbose) if test "$(DO_COMMIT)" = 'yes'; then \
		git diff --quiet -- $(WORKFLOWS) \
		|| git commit -m 'GitHub Actions: Regen workflows' -- $(WORKFLOWS); \
	fi

# The actual recipe which creates the workflow.
#
# There is a condition on the input file name: if it is `*-CT_SUITE.yaml` the
# file will appended once per common_test testsuite. The name of the testsuite
# is replaced.
#
# For all other files, they are appended once in total. There is a special
# handling of lines containing `$(CT_SUITES)`: the line is duplicated for each
# common_test testsuite.
define test_workflow

.PHONY: $(WORKFLOWS_DIR)/test-erlang-otp-$(1).yaml
$(WORKFLOWS_DIR)/test-erlang-otp-$(1).yaml:
	$$(gen_verbose) mkdir -p "$$(dir $$@)"
	$$(verbose) :> "$$@"
	$$(verbose) \
	$$(foreach job,$$(TESTING_JOBS),\
	$$(if $$(filter %/$$(CT_SUITE_JOB_BASENAME),$$(job)),\
	$$(foreach ct_suite,$$(sort $$(CT_SUITES)),\
	sed -E \
	  -e 's/\$$$$\(ERLANG_VERSION\)/$(1)/g' \
	  -e 's/\$$$$\(ELIXIR_VERSION\)/$(ELIXIR_VERSION)/g' \
	  -e 's/\$$$$\(PROJECT\)/$(PROJECT)/g' \
	  -e 's/\$$$$\(RABBITMQ_COMPONENT_REPO_NAME\)/$(RABBITMQ_COMPONENT_REPO_NAME)/g' \
	  -e 's/\$$$$\(base_rmq_ref\)/$(base_rmq_ref)/g' \
	  -e 's/\$$$$\(CT_SUITE\)/$$(ct_suite)/g' \
	  < "$$(job)" >> "$$@";\
	)\
	,\
	sed -E \
	  -e 's/\$$$$\(ERLANG_VERSION\)/$(1)/g' \
	  -e 's/\$$$$\(ELIXIR_VERSION\)/$(ELIXIR_VERSION)/g' \
	  -e 's/\$$$$\(PROJECT\)/$(PROJECT)/g' \
	  -e 's/\$$$$\(RABBITMQ_COMPONENT_REPO_NAME\)/$(RABBITMQ_COMPONENT_REPO_NAME)/g' \
	  -e 's/\$$$$\(base_rmq_ref\)/$(base_rmq_ref)/g' \
	  < "$$(job)" | \
	awk \
	  '\
	  BEGIN { \
	    ct_suites_count = split("$$(CT_SUITES)", ct_suites); \
	  } \
	  /\$$$$\(CT_SUITES\)/ { \
	    for (i = 1; i <= ct_suites_count; i++) { \
	      line = $$$$0; \
	      gsub(/\$$$$\(CT_SUITES\)/, ct_suites[i], line); \
	      print line; \
	    } \
	    next; \
	  } \
	  { print; } \
	  '\
	  >> "$$@";\
	))

endef

$(eval \
$(foreach erlang,$(ERLANG_VERSIONS),\
$(call test_workflow,$(erlang))\
))

clean-github-actions:
	@rm -f $(WORKFLOWS)

endif
