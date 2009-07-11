EBIN_DIR=ebin
SOURCE_DIR=src

SVN_ROOT=http://mochiweb.googlecode.com/svn/trunk
REVISION=102

TMP_DIR=tmp
DEPS_DIR=deps
LIB_PACKAGE=mochiweb
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez


ERLC_OPTS=-o $(EBIN_DIR) -Wall +debug_info

SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES)) $(DEPS_DIR)/$(LIB_PACKAGE_NAME)


all: $(TARGETS)

clean:
	rm -rf $(EBIN_DIR)/*.beam $(LIB_PACKAGE_DIR) $(TARGETS)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl
	erlc $(ERLC_OPTS) $<

$(DEPS_DIR):
	mkdir -p $@

$(LIB_PACKAGE_DIR):
	svn co $(SVN_ROOT) $@

$(DEPS_DIR)/%.ez: $(DEPS_DIR) $(LIB_PACKAGE_DIR)
	(cd $(LIB_PACKAGE_DIR); svn up -r $(REVISION))
	$(MAKE) -C $(LIB_PACKAGE_DIR) clean all
	zip -r $(DEPS_DIR)/$(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/*
