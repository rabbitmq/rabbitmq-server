PACKAGE=mod_http
PACKAGE_NAME=$(PACKAGE).ez

EBIN_DIR=ebin
SOURCE_DIR=src
DIST_DIR=dist
TEST_DIR=test
TEST_EBIN=test_ebin

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

ERLC_OPTS=-o $(EBIN_DIR) -Wall +debug_info
TEST_ERLC_OPTS=-o $(TEST_EBIN)

SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES)) $(DEPS_DIR)/$(LIB_PACKAGE_NAME)
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_EBIN)/%.beam, $(TEST_SOURCES))

all: $(TARGETS) $(TEST_TARGETS)

clean:
	rm -rf $(EBIN_DIR)/*.beam $(LIB_PACKAGE_DIR) $(DIST_DIR) $(TARGETS)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl
	erlc $(ERLC_OPTS) $<
	
$(TEST_EBIN):
	mkdir -p $@

$(TEST_DIR)/%.erl: $(TEST_EBIN)

$(TEST_EBIN)/%.beam: $(TEST_DIR)/%.erl
	erlc $(TEST_ERLC_OPTS) $<

compile_tests: $(TEST_EBIN)/%.beam

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

package: $(DIST_DIR)/$(PACKAGE_NAME)

install: package $(DEPS_DIR)/$(LIB_PACKAGE_NAME)
	mkdir -p $(PLUGINS_DIR)
	mkdir -p $(PLUGINS_LIB_DIR)
	cp $(DIST_DIR)/$(PACKAGE_NAME) $(PLUGINS_DIR)
	cp $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(PLUGINS_LIB_DIR)
