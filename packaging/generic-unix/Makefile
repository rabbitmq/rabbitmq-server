VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_TARBALL=rabbitmq-server-generic-unix-$(VERSION)

dist:
	tar -zxf ../../dist/$(SOURCE_DIR).tar.gz

	$(MAKE) -C $(SOURCE_DIR) \
		TARGET_DIR=`pwd`/$(TARGET_DIR) \
		SBIN_DIR=`pwd`/$(TARGET_DIR)/sbin \
		MAN_DIR=`pwd`/$(TARGET_DIR)/share/man \
			install

	sed -e 's:^SYS_PREFIX=$$:SYS_PREFIX=\$${RABBITMQ_HOME}:' \
        $(TARGET_DIR)/sbin/rabbitmq-defaults >$(TARGET_DIR)/sbin/rabbitmq-defaults.tmp \
        && mv $(TARGET_DIR)/sbin/rabbitmq-defaults.tmp $(TARGET_DIR)/sbin/rabbitmq-defaults
	chmod 0755 $(TARGET_DIR)/sbin/rabbitmq-defaults

	mkdir -p $(TARGET_DIR)/etc/rabbitmq

	tar -zcf $(TARGET_TARBALL).tar.gz $(TARGET_DIR)
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)

clean: clean_partial
	rm -f rabbitmq-server-generic-unix-*.tar.gz

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
