RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client eldap-wrapper

ifeq ($(shell nmap -p 389 localhost | grep '389/tcp open' > /dev/null && echo true),true)
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_auth_backend_ldap_test,[verbose])
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
endif
