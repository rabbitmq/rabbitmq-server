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

.PHONY: update-erlang-mk update-rabbitmq-components.mk

update-erlang-mk: erlang-mk
	$(verbose) if test "$(DO_COMMIT)" = 'yes'; then \
		git diff --quiet -- erlang.mk \
		|| git commit -m 'Update erlang.mk' -- erlang.mk; \
	fi
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		! test -f $$repo/erlang.mk \
		|| $(MAKE) -C $$repo erlang-mk; \
		if test "$(DO_COMMIT)" = 'yes'; then \
			(cd $$repo; \
			 git diff --quiet -- erlang.mk \
			 || git commit -m 'Update erlang.mk' -- erlang.mk); \
		fi; \
	done

# --------------------------------------------------------------------
# rabbitmq-components.mk checks.
# --------------------------------------------------------------------

UPSTREAM_RMQ_COMPONENTS_MK = $(DEPS_DIR)/rabbit_common/mk/rabbitmq-components.mk

ifeq ($(PROJECT),rabbit_common)
check-rabbitmq-components.mk:
	@:
else
check-rabbitmq-components.mk:
	$(verbose) cmp -s rabbitmq-components.mk \
		$(UPSTREAM_RMQ_COMPONENTS_MK) || \
		(echo "error: rabbitmq-components.mk must be updated!" 1>&2; \
		  false)
endif

ifeq ($(PROJECT),rabbit_common)
rabbitmq-components-mk:
	@:
else
rabbitmq-components-mk:
ifeq ($(FORCE),yes)
	$(gen_verbose) cp -a $(UPSTREAM_RMQ_COMPONENTS_MK) .
else
	$(gen_verbose) if test -d .git && test -d $(DEPS_DIR)/rabbit_common/.git; then \
		upstream_branch=$$(LANG=C git -C $(DEPS_DIR)/rabbit_common branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		local_branch=$$(LANG=C git branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		test "$$local_branch" = "$$upstream_branch" || exit 0; \
	fi; \
	cp -a $(UPSTREAM_RMQ_COMPONENTS_MK) .
endif
ifeq ($(DO_COMMIT),yes)
	$(verbose) git diff --quiet rabbitmq-components.mk \
	|| git commit -m 'Update rabbitmq-components.mk' rabbitmq-components.mk
endif
endif

update-rabbitmq-components-mk: rabbitmq-components-mk
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		! test -f $$repo/rabbitmq-components.mk \
		|| $(MAKE) -C $$repo rabbitmq-components-mk; \
	done

update-contributor-code-of-conduct:
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		cp $(DEPS_DIR)/rabbit_common/CODE_OF_CONDUCT.md $$repo/CODE_OF_CONDUCT.md; \
		cp $(DEPS_DIR)/rabbit_common/CONTRIBUTING.md $$repo/CONTRIBUTING.md; \
	done

ifdef CREDS
define replace_aws_creds
	set -e; \
	if test -f "$(CREDS)"; then \
	  key_id=$(shell travis encrypt --no-interactive \
	    "AWS_ACCESS_KEY_ID=$$(awk '/^rabbitmq-s3-access-key-id/ { print $$2; }' < "$(CREDS)")"); \
	  access_key=$(shell travis encrypt --no-interactive \
	    "AWS_SECRET_ACCESS_KEY=$$(awk '/^rabbitmq-s3-secret-access-key/ { print $$2; }' < "$(CREDS)")"); \
	  mv .travis.yml .travis.yml.orig; \
	  awk "\
	  /^  global:/ { \
	    print; \
	    print \"    - secure: $$key_id\"; \
	    print \"    - secure: $$access_key\"; \
	    next; \
	  } \
	  /- secure:/ { next; } \
	  { print; }" < .travis.yml.orig > .travis.yml; \
	  rm -f .travis.yml.orig; \
	else \
	  echo "        INFO: CREDS file missing; not setting/updating AWS credentials"; \
	fi
endef
else
define replace_aws_creds
	echo "        INFO: CREDS not set; not setting/updating AWS credentials"
endef
endif

ifeq ($(PROJECT),rabbit_common)
travis-yml:
	$(gen_verbose) $(replace_aws_creds)
else
travis-yml:
	$(gen_verbose) \
	set -e; \
	if test -d .git && test -d $(DEPS_DIR)/rabbit_common/.git; then \
		upstream_branch=$$(LANG=C git -C $(DEPS_DIR)/rabbit_common branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		local_branch=$$(LANG=C git branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		test "$$local_branch" = "$$upstream_branch" || exit 0; \
	fi; \
	test -f .travis.yml || exit 0; \
	(grep -E -- '- secure:' .travis.yml || :) > .travis.yml.creds; \
	cp -a $(DEPS_DIR)/rabbit_common/.travis.yml .travis.yml.orig; \
	awk ' \
	/^  global:/ { \
	  print; \
	  system("test -f .travis.yml.creds && cat .travis.yml.creds"); \
	  next; \
	} \
	/- secure:/ { next; } \
	{ print; } \
	' < .travis.yml.orig > .travis.yml; \
	rm -f .travis.yml.orig .travis.yml.creds; \
	if test -f .travis.yml.patch; then \
		patch -p0 < .travis.yml.patch; \
		rm -f .travis.yml.orig; \
	fi; \
	$(replace_aws_creds)
ifeq ($(DO_COMMIT),yes)
	$(verbose) ! test -f .travis.yml || \
	git diff --quiet .travis.yml \
	|| git commit -m 'Travis CI: Update config from rabbitmq-common' .travis.yml
endif
endif

update-travis-yml: travis-yml
	$(verbose) for repo in $(READY_DEPS:%=$(DEPS_DIR)/%); do \
		! test -f $$repo/rabbitmq-components.mk \
		|| $(MAKE) -C $$repo travis-yml; \
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

.PHONY: sync-gitignore-from-master
sync-gitignore-from-master: $(READY_DEPS:%=$(DEPS_DIR)/%+sync-gitignore-from-master)

%+sync-gitignore-from-master:
	$(gen_verbose) cd $* && \
	if test -d .git; then \
		branch=$$(LANG=C git branch --list | awk '/^\* \(.*detached / {ref=$$0; sub(/.*detached [^ ]+ /, "", ref); sub(/\)$$/, "", ref); print ref; exit;} /^\* / {ref=$$0; sub(/^\* /, "", ref); print ref; exit}'); \
		! test "$$branch" = 'master' || exit 0; \
		git show origin/master:.gitignore > .gitignore; \
	fi
ifeq ($(DO_COMMIT),yes)
	$(verbose) cd $* && \
	if test -d .git; then \
		git diff --quiet .gitignore \
		|| git commit -m 'Git: Sync .gitignore from master' .gitignore; \
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
