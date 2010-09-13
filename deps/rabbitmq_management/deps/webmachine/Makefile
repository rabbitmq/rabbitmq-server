HG_UPSTREAM=http://bitbucket.org/justin/webmachine
REVISION=0c4b60ac68b4

EBIN_DIR=ebin
INCLUDE_DIR=include

LIB_PACKAGE=webmachine
LIB_PACKAGE_DIR=$(LIB_PACKAGE)
LIB_PACKAGE_NAME=$(LIB_PACKAGE).ez

CHECKOUT_DIR=$(LIB_PACKAGE_DIR)-hg

TARGETS=$(LIB_PACKAGE_NAME)

all: $(TARGETS)

clean:
	rm -rf $(EBIN_DIR)
	rm -rf $(INCLUDE_DIR)
	rm -rf $(TARGETS)
	make -C $(LIB_PACKAGE_DIR) clean

distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(LIB_PACKAGE_DIR): $(CHECKOUT_DIR)
	cp -r $< $@

$(CHECKOUT_DIR):
	rm -rf $@-tmp
	hg clone -r $(REVISION) $(HG_UPSTREAM) $@-tmp
	cd $@-tmp && patch -p1 < ../unrebar.patch && patch -p1 < ../uneunit.patch && cd .. && mv $@-tmp $@

%.ez: $(LIB_PACKAGE_DIR)
	$(MAKE) -C $(LIB_PACKAGE_DIR) clean all
	rm -rf ebin include
	cp -r $(LIB_PACKAGE_DIR)/ebin $(EBIN_DIR)
	cp -r $(LIB_PACKAGE_DIR)/include $(INCLUDE_DIR)
	rm -rf $(INCLUDE_DIR)/mochiweb
	zip $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/
	zip -r $(LIB_PACKAGE_NAME) $(LIB_PACKAGE_DIR)/$(EBIN_DIR)/

echo-revision:
	@echo $(REVISION)

