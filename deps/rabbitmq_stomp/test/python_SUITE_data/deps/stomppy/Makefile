UPSTREAM_GIT=https://github.com/jasonrbriggs/stomp.py.git
REVISION=v6.1.0

LIB_DIR=stomppy
CHECKOUT_DIR=stomppy-git

TARGETS=$(LIB_DIR)

all: $(TARGETS)

clean:
	rm -rf $(LIB_DIR)

distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(LIB_DIR) : $(CHECKOUT_DIR)
	rm -rf $@
	cp -R $< $@

$(CHECKOUT_DIR):
	git clone $(UPSTREAM_GIT) $@
	(cd $@ && git checkout $(REVISION)) || rm -rf $@

echo-revision:
	@echo $(REVISION)

