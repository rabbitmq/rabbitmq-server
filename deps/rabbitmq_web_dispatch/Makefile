EBIN_DIR=ebin


TMP_DIR=tmp
DEPS_DIR=deps
LIB_PACKAGE=mochiweb

all:
	(cd src;$(MAKE))

clean:
	(cd src;$(MAKE) clean)


libs:
	rm -rf $(TMP_DIR)
	svn co http://mochiweb.googlecode.com/svn/trunk $(TMP_DIR)
	$(MAKE) -C $(TMP_DIR)
	(cd $(TMP_DIR); mkdir $(LIB_PACKAGE); cp -r $(EBIN_DIR) \
        $(LIB_PACKAGE); zip -r $(LIB_PACKAGE).ez $(LIB_PACKAGE))
	mkdir -p $(DEPS_DIR)
	cp $(TMP_DIR)/$(LIB_PACKAGE).ez $(DEPS_DIR)

