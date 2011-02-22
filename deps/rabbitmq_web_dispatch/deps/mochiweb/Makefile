UPSTREAM_GIT=http://github.com/mochi/mochiweb.git
REVISION=9a53dbd7b2c52eb5b9d4

EBIN_DIR=ebin

LIB_PACKAGE=mochiweb
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez

CHECKOUT_DIR=$(LIB_PACKAGE_DIR)-git

TARGETS=$(LIB_PACKAGE_NAME)

all: $(TARGETS)

clean:
	rm -rf $(EBIN_DIR)
	rm -rf $(INCLUDE_DIR)
	rm -rf $(LIB_PACKAGE_DIR) $(TARGETS)

distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(LIB_PACKAGE_DIR): $(CHECKOUT_DIR)
	cp -r $< $@
	cd $@ && patch -p1 < ../mochiweb-12b3.patch && patch -p1 < ../10-crypto.patch

$(CHECKOUT_DIR):
	git clone $(UPSTREAM_GIT) $@
	(cd $@ && git checkout $(REVISION)) || rm -rf $@

%.ez: $(LIB_PACKAGE_DIR)
	$(MAKE) -C $(LIB_PACKAGE_DIR) clean all
	rm -rf ebin include
	cp -r $(LIB_PACKAGE_DIR)/ebin $(EBIN_DIR)
	zip $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/
	zip -r $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/

echo-revision:
	@echo $(REVISION)

