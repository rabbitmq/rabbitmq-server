UPSTREAM_HG=https://stomppy.googlecode.com/hg/
REVISION=16a4000624a7

LIB_DIR=stomppy
CHECKOUT_DIR=stomppy-hg

TARGETS=$(LIB_DIR)

all: $(TARGETS)

clean:
	rm -rf $(LIB_DIR)

distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(LIB_DIR) : $(CHECKOUT_DIR) rabbit.patch
	rm -rf $@
	cp -R $< $@
	cd $@ && patch -p1 < ../rabbit.patch

$(CHECKOUT_DIR):
	hg clone $(UPSTREAM_HG) $@
	(cd $@ && hg up $(REVISION)) || rm -rf $@

echo-revision:
	@echo $(REVISION)

