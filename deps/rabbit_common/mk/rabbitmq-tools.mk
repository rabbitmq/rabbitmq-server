READY_DEPS = $(foreach DEP,\
	       $(filter $(RABBITMQ_COMPONENTS),$(DEPS) $(BUILD_DEPS) $(TEST_DEPS)), \
	       $(if $(wildcard $(DEPS_DIR)/$(DEP)),$(DEP),))

RELEASED_RMQ_DEPS = $(filter $(RABBITMQ_COMPONENTS),$(DEPS) $(BUILD_DEPS))

update-contributor-code-of-conduct:
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		cp $(DEPS_DIR)/rabbit_common/CODE_OF_CONDUCT.md $$repo/CODE_OF_CONDUCT.md; \
		cp $(DEPS_DIR)/rabbit_common/CONTRIBUTING.md $$repo/CONTRIBUTING.md; \
	done

# --------------------------------------------------------------------
# erlang.mk query-deps* formatting.
# --------------------------------------------------------------------

# We need to provide a repo mapping for deps resolved via git_rmq fetch method
query_repo_git_rmq = https://github.com/rabbitmq/$(call rmq_cmp_repo_name,$(1))
