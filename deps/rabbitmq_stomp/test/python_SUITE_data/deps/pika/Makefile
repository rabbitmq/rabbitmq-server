UPSTREAM_GIT=https://github.com/pika/pika.git
REVISION=1.1.0

LIB_DIR=pika
CHECKOUT_DIR=pika-$(REVISION)

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
	git clone --depth 1 --branch $(REVISION) $(UPSTREAM_GIT) $@ || \
		(rm -rf $@; exit 1)

echo-revision:
	@echo $(REVISION)

