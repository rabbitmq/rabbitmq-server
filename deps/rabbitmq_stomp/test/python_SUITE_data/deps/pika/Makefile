UPSTREAM_GIT=https://github.com/pika/pika.git
REVISION=0.12.0

LIB_DIR=pika
CHECKOUT_DIR=pika-git

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

