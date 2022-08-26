.ONESHELL:# single shell invocation for all lines in the recipe
SHELL = bash# we depend on bash expansion for e.g. queue patterns

.DEFAULT_GOAL = help

ifndef TEST_ARGS
override TEST_ARGS = "test/oauth/with-uaa"
endif

SELENIUM_VERSION := 103.0

### TARGETS ###

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

wait-for-uaa: ## Wait till UAA is ready
	@(bin/waitTillUAAReady)

run-chrome: ## Install Selenium Chrome
	@(echo "Installing Selenium Chrome")
	@(docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net)
	@(docker rm -f selenium 2>/dev/null || echo "selenium was not running")
	@(docker run -d --name selenium --net rabbitmq_net -p 4444:4444 --shm-size=2g selenium/standalone-chrome:$(SELENIUM_VERSION))
	#@(docker run -d --name selenium --net rabbitmq_selenimum_net -p 4444:4444 --shm-size=2g selenium/standalone-chrome)
	@(echo "Selenium Standalone is running on http://localhost:4444/ && http://selenium:4444/")

build-test-runner-image: ## Prepare docker image to run tests
	@(docker build -t mocha-test  --target test .)

stop-uaa: ## Stop uaa
	@(docker kill uaa && docker rm uaa)

local-setup: ## Deploy RAbbitMQ and UAA to run the test locally using localhost
	@(test/oauth/with-uaa/setup.sh)

remote-setup: ## Deploy RAbbitMQ and UAA to run the test in selenium hub
	@(RABBITMQ_CONFIG_FILE=rabbitmq-remote.config UAA_CONFIG_FOLDER=uaa-remote test/oauth/with-uaa/setup.sh)

run-remote-test: ## Run tests using selenium hub (headless mode)
	@(docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net)
	@(echo "Running selenium tests ...")
	@(docker run --rm  --name mocha --net rabbitmq_net \
	  --env RABBITMQ_URL=http://rabbitmq:15672 \
		--env RUN_LOCAL=false \
		-v ${PWD}/screens:/screens \
		-v ${PWD}/test:/code/test mocha-test $(TEST_ARGS)

		)

run-local-test: ## Run tests using local browser, e.g. run-local-test TEST=oauth/with-uaa/landing.js
	#@(test/oauth/with-uaa/setup.sh)
	@(RABBITMQ_URL=http://localhost:15672 RUN_LOCAL=true SCREENSHOTS_DIR=${PWD}/screens npm test $(TEST_ARGS))
