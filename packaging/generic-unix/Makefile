TARBALL_DIR ?= ../..
TARBALL ?= $(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.xz))
VERSION ?= $(patsubst rabbitmq-server-%.tar.xz,%,$(TARBALL))

SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_TARBALL=rabbitmq-server-generic-unix-$(VERSION)

all: dist
	@:

dist:
	tar -Jxf $(TARBALL_DIR)/$(TARBALL)

	$(MAKE) -C $(SOURCE_DIR) \
		PREFIX= RMQ_ROOTDIR= \
		RMQ_ERLAPP_DIR=`pwd`/$(TARGET_DIR) \
		MANDIR=`pwd`/$(TARGET_DIR)/share/man \
		manpages install install-man

	sed -e 's:^SYS_PREFIX=$$:SYS_PREFIX=\$${RABBITMQ_HOME}:' \
        $(TARGET_DIR)/sbin/rabbitmq-defaults >$(TARGET_DIR)/sbin/rabbitmq-defaults.tmp \
        && mv $(TARGET_DIR)/sbin/rabbitmq-defaults.tmp $(TARGET_DIR)/sbin/rabbitmq-defaults
	chmod 0755 $(TARGET_DIR)/sbin/rabbitmq-defaults

	mkdir -p $(TARGET_DIR)/etc/rabbitmq

	tar -zcf $(TARGET_TARBALL).tar.gz $(TARGET_DIR)
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(TARGET_TARBALL).tar.gz "$(PACKAGES_DIR)"; \
	fi

clean: clean_partial
	rm -f rabbitmq-server-generic-unix-*.tar.gz

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
