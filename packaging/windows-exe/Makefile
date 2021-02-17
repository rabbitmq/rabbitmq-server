ifeq ($(PACKAGES_DIR),)
ZIP_DIR = ../windows
else
ZIP_DIR = $(PACKAGES_DIR)
endif
ZIP = $(notdir $(wildcard $(ZIP_DIR)/rabbitmq-server-windows-*.zip))

VERSION = $(patsubst rabbitmq-server-windows-%.zip,%,$(ZIP))
PRODUCT_VERSION ?= $(shell echo "$(VERSION)" | sed -E 's/([0-9]+\.[0-9]+\.[0-9]+).*/\1.0/')

unexport DEPS_DIR
unexport ERL_LIBS
MAKEOVERRIDES =

all: dist
	@:

dist: rabbitmq-$(VERSION).nsi rabbitmq_server-$(VERSION)
	makensis -V2 rabbitmq-$(VERSION).nsi

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv rabbitmq-server-$(VERSION).exe "$(PACKAGES_DIR)"; \
	fi

rabbitmq-$(VERSION).nsi: rabbitmq_nsi.in
	sed \
	    -e 's|%%VERSION%%|$(VERSION)|' \
	    -e 's|%%PRODUCT_VERSION%%|$(PRODUCT_VERSION)|' \
	    $< > $@

rabbitmq_server-$(VERSION):
	unzip -q $(ZIP_DIR)/$(ZIP)

clean:
	rm -rf rabbitmq-*.nsi rabbitmq_server-* rabbitmq-server-*.exe
