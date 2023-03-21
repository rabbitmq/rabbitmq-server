.ONESHELL:# single shell invocation for all lines in the recipe
SHELL = bash# we depend on bash expansion for e.g. queue patterns

.DEFAULT_GOAL = help
RABBITMQ_SERVER_ROOT = ../../../../../
SCREENSHOTS_DIR = ${PWD}/../../screens
RABBITMQ_CONFIG_FILES ?= rabbitmq.tmpl.conf

username := rabbit_admin
password := rabbit_admin



### TARGETS ###

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

start-rabbitmq: ## Start RabbitMQ
	@(docker kill rabbitmq >/dev/null 2>&1  && docker rm rabbitmq)
	@(mkdir -p /tmp/rabbitmq)
	@(../gen-env-file ${PWD} /tmp/rabbitmq/.env)
	@(../template-config ${PWD} /tmp/rabbitmq/.env ${RABBITMQ_CONFIG_FILES} /tmp/rabbitmq/generated.conf)
	@(echo "Templated ${RABBITMQ_CONFIG_FILES} into /tmp/rabbitmq/generated.conf ")
	@(gmake --directory=${RABBITMQ_SERVER_ROOT} run-broker \
		RABBITMQ_ENABLED_PLUGINS="rabbitmq_auth_backend_oauth2,rabbitmq_management" \
		RABBITMQ_CONFIG_FILE="/tmp/rabbitmq/generated.conf")

start-uaa: ## Start UAA
	@(echo "Templating ...")
	@(mkdir -p /tmp/rabbitmq)
	@(../gen-env-file ${PWD} /tmp/rabbitmq/.env)
	@(../template-config ${PWD} /tmp/rabbitmq/.env uaa/uaa.tmpl.yml uaa/uaa.yml)
	@(echo "Templated uaa/uaa.tmpl.yml into uaa/uaa.yml ")
	@(UAA_CONFIG=${PWD}/uaa ./start-uaa.sh)

stop-uaa: ## Stop UAA
	@(./stop-uaa.sh)

test: ## Run tests interactively e.g. make test [TEST=landing.js]
	@(RABBITMQ_URL=http://localhost:15672 RUN_LOCAL=true SCREENSHOTS_DIR=${SCREENSHOTS_DIR} npm test $(PWD)/$(TEST))

new-token: ## Create a token for a user. e.g. make token username=rabbit_admin password=rabbit_admin
	@./token  $(username) $(password)

start-keycloak: ## Start keycloak
	@(./start-keycloak.sh)
stop-keycloak: ## Stop keyclaok
	@(docker kill keycloak 2>/dev/null && docker rm keycloak)

start-fakeportal: ## Start fake portal
	@(RABBITMQ_URL=http://localhost:15672 UAA_URL=http://localhost:8080 CLIENT_ID=rabbit_idp_user CLIENT_SECRET=rabbit_idp_user npm run fakeportal)

delete-screenshots: ## Delete all screenshots
	@(rm -fr ${SCREENSHOTS_DIR}/*)
