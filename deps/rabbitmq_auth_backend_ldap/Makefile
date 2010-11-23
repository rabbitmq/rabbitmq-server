PACKAGE=rabbitmq-auth-backend-ldap
APPNAME=rabbit_auth_backend_ldap
DEPS=rabbitmq-server rabbitmq-erlang-client

ELDAP_DIR=eldap
ELDAP_URI=https://github.com/etnt/eldap.git
ELDAP_REVISION=e309de4db4b78d67d623

EXTRA_TARGETS=$(EBIN_DIR)/eldap.beam

include ../include.mk

$(EBIN_DIR)/eldap.beam: $(ELDAP_DIR)/$(EBIN_DIR)/eldap.beam
	cp $(ELDAP_DIR)/$(EBIN_DIR)/*.beam $(EBIN_DIR)

$(ELDAP_DIR)/$(EBIN_DIR)/eldap.beam: $(ELDAP_DIR)
	make -C $(ELDAP_DIR)

$(EBIN_DIR)/rabbit_auth_backend_ldap.beam:: $(ELDAP_DIR)/include/eldap.hrl

$(ELDAP_DIR)/include/eldap.hrl: $(ELDAP_DIR)

$(ELDAP_DIR):
	git clone $(ELDAP_URI) $@
	(cd $@ && git checkout $(ELDAP_REVISION)) || rm -rf $@

clean::
	rm -rf eldap
