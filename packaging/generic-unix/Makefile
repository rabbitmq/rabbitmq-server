VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_TARBALL=rabbitmq-server-generic-unix-$(VERSION)

dist:
	make -C ../.. VERSION=$(VERSION) srcdist
	tar -zxvf ../../dist/$(SOURCE_DIR).tar.gz

	make -C $(SOURCE_DIR) \
		TARGET_DIR=`pwd`/$(TARGET_DIR) \
		SBIN_DIR=`pwd`/$(TARGET_DIR)/sbin \
		MAN_DIR=`pwd`/$(TARGET_DIR)/share/man \
			install

	tar -zcf $(TARGET_TARBALL).tar.gz $(TARGET_DIR)
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)

clean: clean_partial
	rm -f rabbitmq-server-generic-unix-*.tar.gz

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
