ifeq ($(PACKAGES_DIR),)
ZIP_DIR = ../windows
else
ZIP_DIR = $(PACKAGES_DIR)
endif
ZIP = $(notdir $(wildcard $(ZIP_DIR)/rabbitmq-server-windows-[0-9.]*.zip))

VERSION = $(patsubst rabbitmq-server-windows-%.zip,%,$(ZIP))

unexport DEPS_DIR

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
	    $< > $@

rabbitmq_server-$(VERSION):
	unzip -q $(ZIP_DIR)/$(ZIP)

clean:
	rm -rf rabbitmq-*.nsi rabbitmq_server-* rabbitmq-server-*.exe
