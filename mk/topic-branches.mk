SCRATCH := $(CURDIR)/topic-branch-scratch

GIT_FILTER_REPO := $(CURDIR)/bin/git-filter-repo

$(GIT_FILTER_REPO):
    mkdir -p $(TMPDIR) \
	&& cd $(TMPDIR) \
	&& curl -LO https://github.com/newren/git-filter-repo/releases/download/v2.28.0/git-filter-repo-2.28.0.tar.xz \
	&& tar -xJf git-filter-repo-*.tar.xz \
	&& mkdir -p $(CURDIR)/bin \
	&& cp git-filter-repo-*/git-filter-repo $(GIT_FILTER_REPO) \
	&& chmod +x $(GIT_FILTER_REPO)

.PHONY: clean-state
clean-state:
	@git diff-index --quiet HEAD -- \
	|| (echo "Cannot proceed with uncommitted changes"; exit 1)

PARENT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

define fetch_topic_branch
echo "Collecting commits from $(1)/$(2)..." \
&& git clone --quiet git@github.com:rabbitmq/$(call rmq_cmp_repo_name,$(1)).git $(SCRATCH)/$(2)/repo-$(1) \
&& cd $(SCRATCH)/$(2)/repo-$(1) \
&& $(GIT_FILTER_REPO) --quiet --to-subdirectory-filter deps/$(1) \
&& git checkout $(2) \
&& git format-patch $(PARENT_BRANCH) \
&& mkdir -p $(SCRATCH)/$(2)/$(1) \
&& cp *.patch $(SCRATCH)/$(2)/$(1) \
|| printf "Topic branch $(2) does not appear to exist in $(1).\n\n";
endef

define rebase_topic_branch
git am --reject $(sort $(wildcard $(1)/*.patch));
endef

fetch-topic-branch-%: $(GIT_FILTER_REPO)
	$(eval TOPIC_BRANCH := $(subst fetch-topic-branch-,,$@))
	mkdir -p $(SCRATCH)/$(TOPIC_BRANCH)
	@$(foreach dep,$(VENDORED_COMPONENTS),$(call fetch_topic_branch,$(dep),$(TOPIC_BRANCH)))
	rm -rf $(SCRATCH)/$(TOPIC_BRANCH)/repo-*

topic-branch-%: $(GIT_FILTER_REPO) clean-state
	$(eval TOPIC_BRANCH := $(subst topic-branch-,,$@))
	ls $(SCRATCH)/$(TOPIC_BRANCH) \
		|| (echo "Fetch the branch first with 'make fetch-$@')"; exit 1)
	git checkout -b $(TOPIC_BRANCH)
	@$(foreach dir,$(wildcard $(SCRATCH)/$(TOPIC_BRANCH)/*),$(call rebase_topic_branch,$(dir)))
