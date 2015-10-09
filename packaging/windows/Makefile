TARBALL_DIR=../..
TARBALL=$(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.xz))
VERSION=$(patsubst rabbitmq-server-%.tar.xz,%,$(TARBALL))

SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_ZIP=rabbitmq-server-windows-$(VERSION)

all: dist
	@:

dist:
	tar -Jxf $(TARBALL_DIR)/$(TARBALL)
	$(MAKE) -C $(SOURCE_DIR) install-windows DESTDIR=$(abspath $(TARGET_DIR)) WINDOWS_PREFIX=

	cp -a README-etc $(TARGET_DIR)/etc/README.txt

	zip -q -r $(TARGET_ZIP).zip $(TARGET_DIR)
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(TARGET_ZIP).zip "$(PACKAGES_DIR)"; \
	fi

clean: clean_partial
	rm -f rabbitmq-server-windows-*.zip

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
