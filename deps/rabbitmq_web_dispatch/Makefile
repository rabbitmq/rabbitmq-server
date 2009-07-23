include ../include.mk

PACKAGE=mod_http
PACKAGE_NAME=$(PACKAGE).ez

JSON_APP=rfc4627_jsonrpc
JSON_APP_ARCHIVE=$(JSON_APP).ez
JSON_DIR=../erlang-rfc4627

PRIV_DIR=priv

TEST_DIR=test
TEST_EBIN_DIR=test_ebin
TEST_INCLUDE_DIR=$(JSON_DIR)/$(INCLUDE_DIR)
TEST_PACKAGE=mod_http_test
TEST_PACKAGE_NAME=$(TEST_PACKAGE).ez

RABBIT_SERVER=../rabbitmq-server
PLUGINS_DIR=$(RABBIT_SERVER)/plugins
PLUGINS_LIB_DIR=$(PLUGINS_DIR)/lib

SVN_ROOT=http://mochiweb.googlecode.com/svn/trunk
REVISION=102

TMP_DIR=tmp
DEPS_DIR=deps
LIB_PACKAGE=mochiweb
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez

TEST_ERLC_OPTS=-o $(TEST_EBIN_DIR) -I $(TEST_INCLUDE_DIR)

ERL=ERL_LIBS=$(DEPS_DIR):$(DIST_DIR):$(JSON_APP) erl

TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES)) $(DEPS_DIR)/$(LIB_PACKAGE_NAME)
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_EBIN_DIR)/%.beam, $(TEST_SOURCES))

all: $(TARGETS) $(TEST_TARGETS)

clean: distclean
	rm -rf $(EBIN_DIR)/*.beam $(LIB_PACKAGE_DIR) $(TARGETS) $(TEST_TARGETS)

distclean:
	rm -rf $(DIST_DIR) $(DEPS_DIR) $(PRIV_DIR)/www/$(TEST_PACKAGE)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl
	erlc $(ERLC_OPTS) $<
	
$(TEST_EBIN_DIR):
	mkdir -p $@

$(TEST_DIR)/%.erl: $(TEST_EBIN_DIR)

$(TEST_EBIN_DIR)/%.beam: $(TEST_DIR)/%.erl
	erlc $(TEST_ERLC_OPTS) $<

$(DEPS_DIR):
	mkdir -p $@

$(LIB_PACKAGE_DIR):
	svn co $(SVN_ROOT) $@

$(DEPS_DIR)/%.ez: $(DEPS_DIR) $(LIB_PACKAGE_DIR)
	(cd $(LIB_PACKAGE_DIR); svn up -r $(REVISION))
	$(MAKE) -C $(LIB_PACKAGE_DIR) clean all
	zip $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/
	zip -r $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/

$(DIST_DIR):
	mkdir -p $@

$(DIST_DIR)/$(PACKAGE_NAME): $(DIST_DIR) $(TARGETS)
	mkdir -p $(DIST_DIR)/$(PACKAGE)
	cp -r $(EBIN_DIR) $(DIST_DIR)/$(PACKAGE)
	(cd $(DIST_DIR); zip -r $(PACKAGE_NAME) $(PACKAGE))

$(DIST_DIR)/$(TEST_PACKAGE_NAME): $(DIST_DIR) $(TEST_TARGETS)
	mkdir -p $(DIST_DIR)/$(TEST_PACKAGE)
	cp -r $(TEST_EBIN_DIR) $(DIST_DIR)/$(TEST_PACKAGE)/$(EBIN_DIR)
	cp -r $(PRIV_DIR) $(DIST_DIR)/$(TEST_PACKAGE)
	(cd $(DIST_DIR); zip -r $(TEST_PACKAGE_NAME) $(TEST_PACKAGE))

package: distclean $(DIST_DIR)/$(PACKAGE_NAME)

package_tests: $(DIST_DIR)/$(TEST_PACKAGE_NAME)

install: clean package $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(JSON_APP)/$(JSON_APP_ARCHIVE)
	mkdir -p $(PLUGINS_DIR)
	mkdir -p $(PLUGINS_LIB_DIR)
	cp $(JSON_APP)/$(JSON_APP_ARCHIVE) $(PLUGINS_DIR)
	cp $(DIST_DIR)/$(PACKAGE_NAME) $(PLUGINS_DIR)
	cp $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(PLUGINS_LIB_DIR)

$(JSON_APP):
	mkdir -p $@	
	
$(JSON_APP)/$(JSON_APP_ARCHIVE): $(JSON_APP)
	$(MAKE) -C $(JSON_DIR) package
	cp $(JSON_DIR)/$(DIST_DIR)/$(JSON_APP_ARCHIVE) \
		$(JSON_APP)/$(JSON_APP_ARCHIVE)

test: package package_tests $(JSON_APP)/$(JSON_APP_ARCHIVE)
	$(ERL) -s crypto -s mod_http -eval \
	'application:start(rfc4627_jsonrpc),application:start(mod_http_test)'
	
