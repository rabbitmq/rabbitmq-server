.PHONY: all clean distclean

all:
	@:

clean:
	rm -f *.pyc

distclean: clean
	find . -regex '.*\(~\|#\|\.swp\)' -exec rm {} \;

# Upstream URL for the current project.
RABBITMQ_COMPONENT_REPO_NAME := rabbitmq-codegen
RABBITMQ_UPSTREAM_FETCH_URL ?= https://github.com/rabbitmq/$(RABBITMQ_COMPONENT_REPO_NAME).git
RABBITMQ_UPSTREAM_PUSH_URL ?= git@github.com:rabbitmq/$(RABBITMQ_COMPONENT_REPO_NAME).git

# Current URL for the current project. If this is not a Git clone,
# default to the upstream Git repository.
ifneq ($(wildcard .git),)
git_origin_fetch_url := $(shell git config remote.origin.url)
git_origin_push_url := $(shell git config remote.origin.pushurl || git config remote.origin.url)
RABBITMQ_CURRENT_FETCH_URL ?= $(git_origin_fetch_url)
RABBITMQ_CURRENT_PUSH_URL ?= $(git_origin_push_url)
else
RABBITMQ_CURRENT_FETCH_URL ?= $(RABBITMQ_UPSTREAM_FETCH_URL)
RABBITMQ_CURRENT_PUSH_URL ?= $(RABBITMQ_UPSTREAM_PUSH_URL)
endif

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
