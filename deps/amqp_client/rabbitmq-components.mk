ifeq ($(.DEFAULT_GOAL),)
# Define default goal to `all` because this file defines soem targets
# before the inclusion of erlang.mk leading the wrong target becoming
# the default.
.DEFAULT_GOAL = all
endif

# For RabbitMQ repositories, we want to checkout branches which match
# the parent project. For instance, if the parent project is on a
# release tag, dependencies must be on the same release tag. If the
# parent project is on a topic branch, dependencies must be on the same
# topic branch or fallback to `stable` or `master` whichever was the
# base of the topic branch.

RABBITMQ_REPO_BASE ?= https://github.com/rabbitmq

dep_amqp_client      = git_rmq rabbitmq-erlang-client $(current_rmq_ref) $(base_rmq_ref)
dep_java_client      = git_rmq rabbitmq-java-client $(current_rmq_ref) $(base_rmq_ref)
dep_rabbit           = git_rmq rabbitmq-server $(current_rmq_ref) $(base_rmq_ref)
dep_rabbit_common    = git_rmq rabbitmq-common $(current_rmq_ref) $(base_rmq_ref)
dep_rabbitmq_codegen = git_rmq rabbitmq-codegen $(current_rmq_ref) $(base_rmq_ref)
dep_rabbitmq_shovel  = git_rmq rabbitmq-shovel $(current_rmq_ref) $(base_rmq_ref)

ifeq ($(origin current_rmq_ref),undefined)
ifneq ($(wildcard .git),)
current_rmq_ref := $(shell \
	git describe --tags --exact-match 2>/dev/null || \
	git symbolic-ref -q --short HEAD)
else
current_rmq_ref := master
endif
endif
export current_rmq_ref

ifeq ($(origin base_rmq_ref),undefined)
ifneq ($(wildcard .git),)
base_rmq_ref := $(shell \
	(git rev-parse --verify -q stable >/dev/null && \
	  git merge-base --is-ancestor $$(git merge-base master HEAD) stable && \
	  echo stable) || \
	echo master)
else
base_rmq_ref := master
endif
endif
export base_rmq_ref

dep_rmq_repo = $(if $(dep_$(1)),					\
	       $(RABBITMQ_REPO_BASE)/$(word 2,$(dep_$(1))).git,		\
	       $(pkg_$(1)_repo))
dep_rmq_commits = $(if $(dep_$(1)),					\
		  $(wordlist 3,$(words $(dep_$(1))),$(dep_$(1))),	\
		  $(pkg_$(1)_commit))

define dep_fetch_git_rmq
	git clone -q -n -- \
	  $(call dep_rmq_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)); \
	cd $(DEPS_DIR)/$(call dep_name,$(1)) && ( \
	$(foreach ref,$(call dep_rmq_commits,$(1)), \
	  git checkout -q $(ref) >/dev/null 2>&1 || \
	  ) \
	(echo "error: no valid pathspec among: $(call dep_rmq_commits,$(1))" \
	  1>&2 && false) )
endef

ifeq ($(PROJECT),rabbit_common)
else ifeq ($(IS_DEP),1)
else
deps:: check-rabbitmq-components.mk
list-deps: check-rabbitmq-components.mk
endif

check-rabbitmq-components.mk:
	$(verbose) cmp -s rabbitmq-components.mk \
		$(DEPS_DIR)/rabbit_common/mk/rabbitmq-components.mk || \
		(echo "error: rabbitmq-components.mk must be updated!" 1>&2; \
		  false)
