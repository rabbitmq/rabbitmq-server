PLATFORM := $(shell uname)

ifeq ($(PLATFORM),Darwin)
HOST_DOCKER := host.docker.internal
endif

ifeq ($(PLATFORM),Linux)
HOST_DOCKER := $(shell ip addr show docker0 | grep -Po 'inet \K[\d.]+')
endif

.DEFAULT_GOAL = test

.PHONY: run-uaa
run-uaa:
	docker exec uaa /bin/echo \
	|| docker run \
		--detach \
		--name uaa \
		--publish 8080:8080 \
		--mount type=bind,source=$(CURDIR)/uaa,target=/etc/uaa \
		--env JAVA_OPTS="-Djava.security.egd=file:/dev/./urandom" \
		pkuryloski/uaa:74.15.0

.PHONY: run-rabbitmq
run-rabbitmq: run-uaa
	docker exec rabbitmq /bin/echo \
	|| docker run \
		--detach \
		--name rabbitmq \
		--publish 5672:5672 \
		--publish 15672:15672 \
		--mount type=bind,source="$(CURDIR)"/rabbitmq,target=/etc/rabbitmq \
		rabbitmq:3.8-management

.PHONY: wait-rabbitmq
wait-rabbitmq: run-rabbitmq
	docker exec rabbitmq rabbitmq-diagnostics ping -q \
	|| (sleep 15; docker exec rabbitmq rabbitmq-diagnostics ping -q)

.PHONY: vhosts
vhosts: wait-rabbitmq
	docker exec rabbitmq rabbitmqctl add_vhost uaa_vhost
	docker exec rabbitmq rabbitmqctl add_vhost other_vhost
	docker exec rabbitmq rabbitmqctl set_permissions -p uaa_vhost admin '.*' '.*' '.*'
	docker exec rabbitmq rabbitmqctl set_permissions -p other_vhost admin '.*' '.*' '.*'

.PHONY: uaac-bunny-image
uaac-bunny-image:
	docker images | grep "^uaac\s" \
	|| docker run --name uaac ruby:2.5 gem install cf-uaac bunny
	docker images | grep "^uaac\s" \
	|| (docker commit $$(docker ps -aqf "name=^uaac$$") uaac:latest \
		&& docker rm uaac)

.PHONY: queues
queues: vhosts uaac-bunny-image
	docker run -it --rm \
		--name declare-queues \
		--mount type=bind,source="$(CURDIR)"/../declare_queues.rb,target=/workspace/declare_queues.rb \
		-w /workspace \
		uaac \
		/bin/bash -c "BUNNY_HOST=$(HOST_DOCKER) \
					BUNNY_USER=admin \
					BUNNY_PASS=rabo2 \
				ruby declare_queues.rb \
					uaa_vhost/some_queue \
					uaa_vhost/other_queue \
					other_vhost/some_queue \
					other_vhost/other_queue"

.PHONY: tokens
tokens: queues uaac-bunny-image
	docker run -it --rm \
		--name fetch-tokens \
		uaac \
		/bin/bash -c " \
		uaac target http://$(HOST_DOCKER):8080/uaa \
		&& uaac signing key \
		&& uaac token owner get rabbit_client rabbit_super -s rabbit_secret -p rabbit_super \
		&& uaac token owner get rabbit_client rabbit_nosuper -s rabbit_secret -p rabbit_nosuper \
		&& uaac context rabbit_nosuper \
		&& uaac context rabbit_super"

.PHONY: check-token
check-token: queues uaac-bunny-image
	docker run -it --rm \
		--name check-token \
		--mount type=bind,source="$(CURDIR)"/check_token.rb,target=/workspace/check_token.rb \
		uaac \
		/bin/bash -c " \
		uaac target http://$(HOST_DOCKER):8080/uaa \
		&& uaac token owner get rabbit_client rabbit_super -s rabbit_secret -p rabbit_super \
		&& BUNNY_HOST=$(HOST_DOCKER) ruby /workspace/check_token.rb"

.PHONY: test
test: check-token

.PHONY: cleanup
cleanup:
	docker stop rabbitmq uaa
	docker rm rabbitmq uaa