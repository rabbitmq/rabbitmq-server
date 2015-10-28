SOURCE_DIST_FILE ?= $(wildcard ../../../rabbitmq-server-*.tar.xz)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multile source archives found; please specify SOURCE_DIST_FILE)
endif

VERSION ?= $(patsubst rabbitmq-server-%.tar.xz,%,$(notdir $(SOURCE_DIST_FILE)))
ifeq ($(VERSION),)
$(error Cannot determine version; please specify VERSION)
endif

SOURCE_DIR = rabbitmq-server-$(VERSION)
TARGET_DIR = rabbitmq_server-$(VERSION)
TARGET_ZIP = rabbitmq-server-windows-$(VERSION)

unexport DEPS_DIR

all: dist
	@:

dist:
	tar -Jxf $(SOURCE_DIST_FILE)
	$(MAKE) -C $(SOURCE_DIR) install-windows \
		DESTDIR=$(abspath $(TARGET_DIR)) \
		WINDOWS_PREFIX=

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
