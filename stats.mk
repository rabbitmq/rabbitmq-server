# Used & tested on macOS

DOCKER := /usr/local/bin/docker

define STATS_CONTAINER
$(DOCKER) run \
  --interactive --tty --rm \
  --volume rabbitmq_releases:/home/node \
  node bash
endef
.PHONY: stats
stats:
	$(verbose) $(STATS_CONTAINER) -c "cd /home/node && npm install rels && node_modules/.bin/rels --repo rabbitmq/rabbitmq-server --all"

.PHONY: stats_interactive
stats_interactive:
	$(verbose) $(STATS_CONTAINER)
