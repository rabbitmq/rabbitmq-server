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

.PHONY: show-upstream-git-fetch-url show-upstream-git-push-url \
	show-current-git-fetch-url show-current-git-push-url

show-upstream-git-fetch-url:
	@echo $(RABBITMQ_UPSTREAM_FETCH_URL)

show-upstream-git-push-url:
	@echo $(RABBITMQ_UPSTREAM_PUSH_URL)

show-current-git-fetch-url:
	@echo $(RABBITMQ_CURRENT_FETCH_URL)

show-current-git-push-url:
	@echo $(RABBITMQ_CURRENT_PUSH_URL)

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

.PHONY: sync-gitignore-from-main
sync-gitignore-from-main: $(READY_DEPS:%=$(DEPS_DIR)/%+sync-gitignore-from-main)

%+sync-gitignore-from-main:
	$(gen_verbose) cd $* && \
	if test -d .git; then \
		branch=$$(LANG=C git branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		! test "$$branch" = 'main' || exit 0; \
		git show origin/main:.gitignore > .gitignore; \
	fi
ifeq ($(DO_COMMIT),yes)
	$(verbose) cd $* && \
	if test -d .git; then \
		git diff --quiet .gitignore \
		|| git commit -m 'Git: Sync .gitignore from main' .gitignore; \
	fi
endif

.PHONY: show-branch

show-branch: $(READY_DEPS:%=$(DEPS_DIR)/%+show-branch)
	$(verbose) printf '%-34s %s\n' $(PROJECT): "$$(git symbolic-ref -q --short HEAD || git describe --tags --exact-match)"

%+show-branch:
	$(verbose) printf '%-34s %s\n' $(notdir $*): "$$(cd $* && (git symbolic-ref -q --short HEAD || git describe --tags --exact-match))"

SINCE_TAG ?= last-release
COMMITS_LOG_OPTS ?= --oneline --decorate --no-merges
MARKDOWN ?= no

define show_commits_since_tag
set -e; \
if test "$1"; then \
	erlang_app=$(notdir $1); \
	repository=$(call rmq_cmp_repo_name,$(notdir $1)); \
	git_dir=-C\ "$1"; \
else \
	erlang_app=$(PROJECT); \
	repository=$(call rmq_cmp_repo_name,$(PROJECT)); \
fi; \
case "$(SINCE_TAG)" in \
last-release) \
	tags_count=$$(git $$git_dir tag -l 2>/dev/null | grep -E -v '(-beta|_milestone|[-_]rc)' | wc -l); \
	;; \
*) \
	tags_count=$$(git $$git_dir tag -l 2>/dev/null | wc -l); \
	;; \
esac; \
if test "$$tags_count" -gt 0; then \
	case "$(SINCE_TAG)" in \
	last-release) \
		ref=$$(git $$git_dir describe --abbrev=0 --tags \
			--exclude "*-beta*" \
			--exclude "*_milestone*" \
			--exclude "*[-_]rc*"); \
		;; \
	last-prerelease) \
		ref=$$(git $$git_dir describe --abbrev=0 --tags); \
		;; \
	*) \
		git $$git_dir rev-parse "$(SINCE_TAG)" -- >/dev/null; \
		ref=$(SINCE_TAG); \
		;; \
	esac; \
	commits_count=$$(git $$git_dir log --oneline "$$ref.." | wc -l); \
	if test "$$commits_count" -gt 0; then \
		if test "$(MARKDOWN)" = yes; then \
			printf "\n## [\`$$repository\`](https://github.com/rabbitmq/$$repository)\n\nCommits since \`$$ref\`:\n\n"; \
			git $$git_dir --no-pager log $(COMMITS_LOG_OPTS) \
				--format="format:* %s ([\`%h\`](https://github.com/rabbitmq/$$repository/commit/%H))" \
				"$$ref.."; \
			echo; \
		else \
			echo; \
			echo "# $$repository - Commits since $$ref"; \
			git $$git_dir log $(COMMITS_LOG_OPTS) "$$ref.."; \
		fi; \
	fi; \
else \
	if test "$(MARKDOWN)" = yes; then \
		printf "\n## [\`$$repository\`](https://github.com/rabbitmq/$$repository)\n\n**New** since the last release!\n"; \
	else \
		echo; \
		echo "# $$repository - New since the last release!"; \
	fi; \
fi
endef

.PHONY: commits-since-release

commits-since-release: commits-since-release-title \
    $(RELEASED_RMQ_DEPS:%=$(DEPS_DIR)/%+commits-since-release)
	$(verbose) $(call show_commits_since_tag)

commits-since-release-title:
	$(verbose) set -e; \
	case "$(SINCE_TAG)" in \
	last-release) \
		tags_count=$$(git $$git_dir tag -l 2>/dev/null | grep -E -v '(-beta|_milestone|[-_]rc)' | wc -l); \
		;; \
	*) \
		tags_count=$$(git $$git_dir tag -l 2>/dev/null | wc -l); \
		;; \
	esac; \
	if test "$$tags_count" -gt 0; then \
		case "$(SINCE_TAG)" in \
		last-release) \
			ref=$$(git $$git_dir describe --abbrev=0 --tags \
				--exclude "*-beta*" \
				--exclude "*_milestone*" \
				--exclude "*[-_]rc*"); \
			;; \
		last-prerelease) \
			ref=$$(git $$git_dir describe --abbrev=0 --tags); \
			;; \
		*) \
			ref=$(SINCE_TAG); \
			;; \
		esac; \
		version=$$(echo "$$ref" | sed -E \
			-e 's/rabbitmq_v([0-9]+)_([0-9]+)_([0-9]+)/v\1.\2.\3/' \
			-e 's/_milestone/-beta./' \
			-e 's/_rc/-rc./' \
			-e 's/^v//'); \
		echo "# Changes since RabbitMQ $$version"; \
	else \
		echo "# Changes since the beginning of time"; \
	fi

%+commits-since-release:
	$(verbose) $(call show_commits_since_tag,$*)

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
