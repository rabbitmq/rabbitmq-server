.ONESHELL:# single shell invocation for all lines in the recipe
SHELL = bash# we depend on bash expansion for e.g. queue patterns

.DEFAULT_GOAL = help
RABBITMQ_SERVER_ROOT = ../../../../../


### TARGETS ###

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test: ## Run tests interactively e.g. make test [TEST=landing.js]
	@(RABBITMQ_URL=http://localhost:15672 RUN_LOCAL=true SCREENSHOTS_DIR=${PWD}/../../screens npm test $(PWD)/$(TEST))
