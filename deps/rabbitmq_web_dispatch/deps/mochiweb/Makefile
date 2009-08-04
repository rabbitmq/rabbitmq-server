SVN_ROOT=http://mochiweb.googlecode.com/svn/trunk
REVISION=102

EBIN_DIR=ebin
INCLUDE_DIR=include

LIB_PACKAGE=mochiweb
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez

ERL=ERL_LIBS=$(DEPS_DIR):$(DIST_DIR):$(JSON_APP) erl

TARGETS=$(LIB_PACKAGE_NAME)

all: $(TARGETS) $(TEST_TARGETS)

clean:
	rm -rf $(EBIN_DIR)
	rm -rf $(INCLUDE_DIR)
	rm -rf $(EBIN_DIR)/*.beam $(LIB_PACKAGE_DIR) $(TARGETS) $(TEST_TARGETS)

$(LIB_PACKAGE_DIR):
	svn co $(SVN_ROOT) $@

%.ez: $(LIB_PACKAGE_DIR)
	(cd $(LIB_PACKAGE_DIR); svn up -r $(REVISION))
	$(MAKE) -C $(LIB_PACKAGE_DIR) clean all
	rm -rf ebin include
	cp -r $(LIB_PACKAGE_DIR)/ebin $(EBIN_DIR)
	cp -r $(LIB_PACKAGE_DIR)/include $(INCLUDE_DIR)
	zip $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/
	zip -r $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/

$(DIST_DIR):
	mkdir -p $@

package: $(DIST_DIR)/$(PACKAGE_NAME)

package_tests: $(DIST_DIR)/$(TEST_PACKAGE_NAME)

install: clean package $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(JSON_APP)/$(JSON_APP_ARCHIVE)
	mkdir -p $(PLUGINS_DIR)
	mkdir -p $(PLUGINS_LIB_DIR)
	cp $(JSON_APP)/$(JSON_APP_ARCHIVE) $(PLUGINS_DIR)
	cp $(DIST_DIR)/$(PACKAGE_NAME) $(PLUGINS_DIR)
	cp $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(PLUGINS_LIB_DIR)
