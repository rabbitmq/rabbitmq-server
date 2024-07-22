ifeq ($(PLATFORM),msys2)
HOSTNAME = $(COMPUTERNAME)
else
ifeq ($(PLATFORM),solaris)
HOSTNAME = $(shell hostname | sed 's@\..*@@')
else
HOSTNAME = $(shell hostname -s)
endif
endif

READY_DEPS = $(foreach DEP,\
	       $(filter $(RABBITMQ_COMPONENTS),$(DEPS) $(BUILD_DEPS) $(TEST_DEPS)), \
	       $(if $(wildcard $(DEPS_DIR)/$(DEP)),$(DEP),))

RELEASED_RMQ_DEPS = $(filter $(RABBITMQ_COMPONENTS),$(DEPS) $(BUILD_DEPS))

update-contributor-code-of-conduct:
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		cp $(DEPS_DIR)/rabbit_common/CODE_OF_CONDUCT.md $$repo/CODE_OF_CONDUCT.md; \
		cp $(DEPS_DIR)/rabbit_common/CONTRIBUTING.md $$repo/CONTRIBUTING.md; \
	done

ifneq ($(wildcard .git),)

.PHONY: sync-gitremote sync-gituser

sync-gitremote: $(READY_DEPS:%=$(DEPS_DIR)/%+sync-gitremote)
	@:

%+sync-gitremote:
	$(exec_verbose) cd $* && \
		git remote set-url origin \
		'$(call dep_rmq_repo,$(RABBITMQ_CURRENT_FETCH_URL),$(notdir $*))'
	$(verbose) cd $* && \
		git remote set-url --push origin \
		'$(call dep_rmq_repo,$(RABBITMQ_CURRENT_PUSH_URL),$(notdir $*))'

ifeq ($(origin, RMQ_GIT_GLOBAL_USER_NAME),undefined)
RMQ_GIT_GLOBAL_USER_NAME := $(shell git config --global user.name)
export RMQ_GIT_GLOBAL_USER_NAME
endif
ifeq ($(origin RMQ_GIT_GLOBAL_USER_EMAIL),undefined)
RMQ_GIT_GLOBAL_USER_EMAIL := $(shell git config --global user.email)
export RMQ_GIT_GLOBAL_USER_EMAIL
endif
ifeq ($(origin RMQ_GIT_USER_NAME),undefined)
RMQ_GIT_USER_NAME := $(shell git config user.name)
export RMQ_GIT_USER_NAME
endif
ifeq ($(origin RMQ_GIT_USER_EMAIL),undefined)
RMQ_GIT_USER_EMAIL := $(shell git config user.email)
export RMQ_GIT_USER_EMAIL
endif

sync-gituser: $(READY_DEPS:%=$(DEPS_DIR)/%+sync-gituser)
	@:

%+sync-gituser:
ifeq ($(RMQ_GIT_USER_NAME),$(RMQ_GIT_GLOBAL_USER_NAME))
	$(exec_verbose) cd $* && git config --unset user.name || :
else
	$(exec_verbose) cd $* && git config user.name "$(RMQ_GIT_USER_NAME)"
endif
ifeq ($(RMQ_GIT_USER_EMAIL),$(RMQ_GIT_GLOBAL_USER_EMAIL))
	$(verbose) cd $* && git config --unset user.email || :
else
	$(verbose) cd $* && git config user.email "$(RMQ_GIT_USER_EMAIL)"
endif

endif # ($(wildcard .git),)

# --------------------------------------------------------------------
# erlang.mk query-deps* formatting.
# --------------------------------------------------------------------

# We need to provide a repo mapping for deps resolved via git_rmq fetch method
query_repo_git_rmq = https://github.com/rabbitmq/$(call rmq_cmp_repo_name,$(1))

# --------------------------------------------------------------------
# Common test logs compression.
# --------------------------------------------------------------------

.PHONY: ct-logs-archive clean-ct-logs-archive

ifneq ($(wildcard logs/*),)
TAR := tar
ifeq ($(PLATFORM),freebsd)
TAR := gtar
endif
ifeq ($(PLATFORM),darwin)
TAR := gtar
endif

CT_LOGS_ARCHIVE ?= $(PROJECT)-ct-logs-$(subst _,-,$(subst -,,$(subst .,,$(patsubst ct_run.ct_$(PROJECT)@$(HOSTNAME).%,%,$(notdir $(lastword $(wildcard logs/ct_run.*))))))).tar.xz

ifeq ($(patsubst %.tar.xz,%,$(CT_LOGS_ARCHIVE)),$(CT_LOGS_ARCHIVE))
$(error CT_LOGS_ARCHIVE file must use '.tar.xz' as its filename extension)
endif

ct-logs-archive: $(CT_LOGS_ARCHIVE)
	@:

$(CT_LOGS_ARCHIVE):
	$(gen_verbose) \
	for file in logs/*; do \
	  ! test -L "$$file" || rm "$$file"; \
	done
	$(verbose) \
	$(TAR) -c \
	  --exclude "*/mnesia" \
	  --transform "s/^logs/$(patsubst %.tar.xz,%,$(notdir $(CT_LOGS_ARCHIVE)))/" \
	  -f - logs | \
        xz > "$@"
else
ct-logs-archive:
	@:
endif

clean-ct-logs-archive::
	$(gen_verbose) rm -f $(PROJECT)-ct-logs-*.tar.xz

clean:: clean-ct-logs-archive
