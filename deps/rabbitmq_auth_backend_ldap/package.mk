RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client eldap-wrapper

ifeq ($(shell nc -z localhost 389 && echo true),true)
WITH_BROKER_TEST_COMMANDS:=eunit:test([rabbit_auth_backend_ldap_unit_test,rabbit_auth_backend_ldap_test],[verbose])
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
else
$(warning Not running LDAP tests; no LDAP server found on localhost)
endif
